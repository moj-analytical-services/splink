from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict

from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

from .term_frequencies import compute_all_term_frequencies_sqls

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker


def vertically_concatenate_sql(
    input_tables: Dict[str, SplinkDataFrame],
    salting_required: bool,
    source_dataset_input_column: InputColumn = None,
) -> str:
    """
    Using `input_tables`, create a single table with the columns and
    rows required for linking.

    This table will later be the basis for the generation of pairwise record comparises,
    and will also be used to generate the term frequency adjustment tables.

    If multiple input tables are provided, this single table is the vertical
    concatenation of all the input tables.  In this case, a 'source_dataset' column
    is created.  This is used to uniquely identify rows in the vertical concatenation.
    Without it, ID collisions would be possible leading to ambiguity e.g. if several
    of the input tables have the same ID.
    """

    # Use column order from first table in dict
    df_obj = next(iter(input_tables.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    if salting_required:
        salt_sql = ", random() as __splink_salt"
    else:
        salt_sql = ""

    source_dataset_column_already_exists = False
    if source_dataset_input_column:
        source_dataset_column_already_exists = (
            source_dataset_input_column.unquote().name
            in [c.unquote().name for c in df_obj.columns]
        )

    select_columns_sql = ", ".join(columns)
    if len(input_tables) > 1:
        sqls_to_union = []

        for df_obj in input_tables.values():
            if source_dataset_column_already_exists:
                create_sds_if_needed = ""
            else:
                create_sds_if_needed = f"'{df_obj.templated_name}' as source_dataset,"

            sql = f"""
            select
            {create_sds_if_needed}
            {select_columns_sql}
            {salt_sql}
            from {df_obj.physical_name}
            """
            sqls_to_union.append(sql)
        sql = " UNION ALL ".join(sqls_to_union)
    else:
        sql = f"""
            select {select_columns_sql}
            {salt_sql}
            from {df_obj.physical_name}
            """

    return sql


def enqueue_df_concat_with_tf(linker: Linker, pipeline: CTEPipeline) -> CTEPipeline:
    cache = linker._intermediate_table_cache
    if "__splink__df_concat_with_tf" in cache:
        nodes_with_tf = cache.get_with_logging("__splink__df_concat_with_tf")
        pipeline.append_input_dataframe(nodes_with_tf)
        return pipeline

    sds_ic = linker._settings_obj.column_info_settings.source_dataset_input_column

    sql = vertically_concatenate_sql(
        input_tables=linker._input_tables_dict,
        salting_required=linker._settings_obj.salting_required,
        source_dataset_input_column=sds_ic,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    sqls = compute_all_term_frequencies_sqls(linker, pipeline)
    pipeline.enqueue_list_of_sqls(sqls)

    return pipeline


def compute_df_concat_with_tf(linker: Linker, pipeline: CTEPipeline) -> SplinkDataFrame:
    cache = linker._intermediate_table_cache
    db_api = linker._db_api

    if "__splink__df_concat_with_tf" in cache:
        return cache.get_with_logging("__splink__df_concat_with_tf")

    sds_ic = linker._settings_obj.column_info_settings.source_dataset_input_column

    sql = vertically_concatenate_sql(
        input_tables=linker._input_tables_dict,
        salting_required=linker._settings_obj.salting_required,
        source_dataset_input_column=sds_ic,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    sqls = compute_all_term_frequencies_sqls(linker, pipeline)
    pipeline.enqueue_list_of_sqls(sqls)

    nodes_with_tf = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    cache["__splink__df_concat_with_tf"] = nodes_with_tf
    return nodes_with_tf


def enqueue_df_concat(linker: Linker, pipeline: CTEPipeline) -> CTEPipeline:
    cache = linker._intermediate_table_cache

    if "__splink__df_concat" in cache:
        nodes_with_tf = cache.get_with_logging("__splink__df_concat")
        pipeline.append_input_dataframe(nodes_with_tf)
        return pipeline

    # __splink__df_concat_with_tf is a superset of __splink__df_concat
    # so if it exists, use it instead
    elif "__splink__df_concat_with_tf" in cache:
        nodes_with_tf = cache.get_with_logging("__splink__df_concat_with_tf")
        nodes_with_tf.templated_name = "__splink__df_concat"
        pipeline.append_input_dataframe(nodes_with_tf)
        return pipeline

    sds_ic = linker._settings_obj.column_info_settings.source_dataset_input_column

    sql = vertically_concatenate_sql(
        input_tables=linker._input_tables_dict,
        salting_required=linker._settings_obj.salting_required,
        source_dataset_input_column=sds_ic,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    return pipeline


def compute_df_concat(linker: Linker, pipeline: CTEPipeline) -> SplinkDataFrame:
    cache = linker._intermediate_table_cache
    db_api = linker._db_api

    if "__splink__df_concat" in cache:
        return cache.get_with_logging("__splink__df_concat")
    if "__splink__df_concat_with_tf" in cache:
        df = cache.get_with_logging("__splink__df_concat_with_tf")
        df.templated_name = "__splink__df_concat"
        return df

    sds_ic = linker._settings_obj.column_info_settings.source_dataset_input_column

    sql = vertically_concatenate_sql(
        input_tables=linker._input_tables_dict,
        salting_required=linker._settings_obj.salting_required,
        source_dataset_input_column=sds_ic,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    nodes_with_tf = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    cache["__splink__df_concat"] = nodes_with_tf
    return nodes_with_tf


def concat_table_column_names(linker: Linker) -> list[str]:
    """
    Returns list of column names of the table __splink__df_concat,
    without needing to instantiate the table.
    """
    source_dataset_input_column = (
        linker._settings_obj.column_info_settings.source_dataset_input_column
    )

    input_tables = linker._input_tables_dict
    salting_required = linker._settings_obj.salting_required

    df_obj = next(iter(input_tables.values()))
    columns = df_obj.columns_escaped
    if salting_required:
        columns.append("__splink_salt")

    if len(input_tables) > 1:
        source_dataset_column_already_exists = False
        if source_dataset_input_column:
            source_dataset_column_already_exists = (
                source_dataset_input_column.unquote().name
                in [c.unquote().name for c in df_obj.columns]
            )
        if not source_dataset_column_already_exists:
            columns.append("source_dataset")
    return columns


def split_df_concat_with_tf_into_two_tables_sqls(
    input_tablename: str, source_dataset_col: str, sample_switch: bool = False
) -> list[dict[str, str]]:
    # For the two dataset link only, rather than a self join of
    # __splink__df_concat_with_tf, it's much faster to split the input
    # into two tables, and join (because then Splink doesn't have to evaluate)
    # intra-dataset comparisons.
    # see https://github.com/moj-analytical-services/splink/pull/1359

    sqls = []
    sample_text = "_sample" if sample_switch else ""

    sql = f"""
        select * from {input_tablename}{sample_text}
        where {source_dataset_col} =
            (select min({source_dataset_col}) from {input_tablename}{sample_text})
        """

    sqls.append(
        {
            "sql": sql,
            "output_table_name": f"{input_tablename}{sample_text}_left",
        }
    )

    sql = f"""
        select * from {input_tablename}{sample_text}
        where {source_dataset_col} =
            (select max({source_dataset_col}) from {input_tablename}{sample_text})
        """
    sqls.append(
        {
            "sql": sql,
            "output_table_name": f"{input_tablename}{sample_text}_right",
        }
    )
    return sqls

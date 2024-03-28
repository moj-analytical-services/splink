from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .pipeline import CTEPipeline
from .splink_dataframe import SplinkDataFrame
from .term_frequencies import compute_all_term_frequencies_sqls

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def vertically_concatenate_sql(linker: Linker) -> str:
    """
    Using `input_table_or_tables`, create a single table with the columns and
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
    df_obj = next(iter(linker._input_tables_dict.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    salting_reqiured = False

    source_dataset_col_req = (
        linker._settings_obj.column_info_settings.source_dataset_column_name is not None
    )

    salting_reqiured = linker._settings_obj.salting_required

    # see https://github.com/duckdb/duckdb/discussions/9710
    # in duckdb to parallelise we need salting
    if linker._sql_dialect == "duckdb":
        salting_reqiured = True

    if salting_reqiured:
        salt_sql = ", random() as __splink_salt"
    else:
        salt_sql = ""

    if source_dataset_col_req:
        sqls_to_union = []

        create_sds_if_needed = ""

        for df_obj in linker._input_tables_dict.values():
            if not linker._source_dataset_column_already_exists:
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

    sql = vertically_concatenate_sql(linker)
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    sqls = compute_all_term_frequencies_sqls(linker, pipeline)
    pipeline.enqueue_list_of_sqls(sqls)

    return pipeline


def compute_df_concat_with_tf(linker: Linker, pipeline: CTEPipeline) -> SplinkDataFrame:
    cache = linker._intermediate_table_cache
    db_api = linker.db_api

    if "__splink__df_concat_with_tf" in cache:
        return cache.get_with_logging("__splink__df_concat_with_tf")

    sql = vertically_concatenate_sql(linker)
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

    sql = vertically_concatenate_sql(linker)
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    return pipeline


def compute_df_concat(linker: Linker, pipeline: CTEPipeline) -> SplinkDataFrame:
    cache = linker._intermediate_table_cache
    db_api = linker.db_api

    if "__splink__df_concat" in cache:
        return cache.get_with_logging("__splink__df_concat")
    if "__splink__df_concat_with_tf" in cache:
        df = cache.get_with_logging("__splink__df_concat_with_tf")
        df.templated_name = "__splink__df_concat"
        return df

    sql = vertically_concatenate_sql(linker)
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    nodes_with_tf = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    cache["__splink__df_concat"] = nodes_with_tf
    return nodes_with_tf

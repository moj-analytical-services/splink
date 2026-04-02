from __future__ import annotations

# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107
import logging
import warnings
from dataclasses import replace
from math import ceil, floor
from typing import TYPE_CHECKING, Any, Optional, cast

from splink.internals.comparison_level import ComparisonLevelDetailedRecord
from splink.internals.duckdb.duckdb_helpers.duckdb_helpers import (
    record_dicts_from_relation,
)
from splink.internals.input_column import InputColumn
from splink.internals.misc import join_sql_fragments, join_sql_with_union_all
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


def colname_to_tf_tablename(input_column: InputColumn) -> str:
    column_name_str = input_column.unquote().name.replace(" ", "_")
    return f"__splink__df_tf_{column_name_str}"


def term_frequencies_for_single_column_sql(
    input_column: InputColumn, table_name: str = "__splink__df_concat"
) -> str:
    col_name = input_column.name
    select_cols = [
        col_name,
        f"""
        cast(count(*) as float8) / (
            select count({col_name}) as total
            from {table_name}
        ) as {input_column.tf_name}
        """,
    ]
    select_cols_str = join_sql_fragments(select_cols, ",\n", indent_size=4)

    sql = f"""
    select
{select_cols_str}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def ensure_term_frequencies_for_linker(linker: Linker) -> list[SplinkDataFrame]:
    cache = linker._intermediate_table_cache
    tf_tables = []

    for tf_col in linker._settings_obj._term_frequency_columns:
        tf_table_name = colname_to_tf_tablename(tf_col)
        if tf_table_name not in cache:
            linker.table_management.compute_tf_table(tf_col.name)
        tf_tables.append(cache.get_with_logging(tf_table_name))

    return tf_tables


def append_term_frequencies_to_pipeline(
    linker: Linker, pipeline: CTEPipeline
) -> CTEPipeline:
    for tf_table in ensure_term_frequencies_for_linker(linker):
        pipeline.append_input_dataframe(tf_table)
    return pipeline


def _join_tf_to_input_table_sql(
    linker: Linker,
    input_tablename: str,
    input_table: Optional[SplinkDataFrame] = None,
) -> str:
    input_table_columns = input_table.columns if input_table is not None else []
    select_cols = [f"{input_tablename}.*"]
    left_joins = []

    for col in linker._settings_obj._term_frequency_columns:
        tf_col_obj = linker._settings_obj._input_column(col.tf_name)
        if tf_col_obj in input_table_columns:
            continue

        tbl = colname_to_tf_tablename(col)
        select_cols.append(f"{tbl}.{col.tf_name}")
        left_joins.append(
            f"left join {tbl} on {input_tablename}.{col.name} = {tbl}.{col.name}"
        )

    select_cols_str = join_sql_fragments(select_cols, ",\n", indent_size=4)
    left_joins_str = "\n".join(left_joins)

    sql = f"""
    select
{select_cols_str}
    from {input_tablename}
    {left_joins_str}
    """

    return sql


def _join_tf_to_df_concat_sql(linker: Linker) -> str:
    return _join_tf_to_input_table_sql(linker, "__splink__df_concat")


def _join_new_table_to_df_concat_with_tf_sql(
    linker: Linker,
    input_tablename: str,
    input_table: Optional[SplinkDataFrame] = None,
) -> str:
    """
    Joins any required tf columns onto input_tablename

    This is needed e.g. when using linker.inference.compare_two_records
    or linker.inference.find_matches_to_new_records in which the user provides
    new records which need tf adjustments computed
    """
    return _join_tf_to_input_table_sql(linker, input_tablename, input_table)


def comparison_level_to_tf_chart_data_sql(
    cl: ComparisonLevelDetailedRecord, sdf: SplinkDataFrame, input_col: InputColumn
) -> str:
    sql = f"""
    SELECT
        {input_col.name} AS value,
        {input_col.tf_name} AS tf,
        {cl.u_probability}::DOUBLE AS u_probability,
        {cl.tf_adjustment_weight}::DOUBLE AS tf_adjustment_weight,
        -- TF match weight scaled by tf_adjustment_weight
        log2(u_probability/tf) * tf_adjustment_weight AS log2_bf_tf,
        -- Tidy up columns
        {cl.comparison_vector_value} AS gamma,
        '{input_col.unquote().name}' AS tf_col,
        {cl.log2_bayes_factor}::DOUBLE AS log2_bf,
        log2_bf_tf + log2_bf AS log2_bf_final,
        row_number() OVER (
                PARTITION BY gamma
                ORDER BY log2_bf_tf
            ) AS most_freq_rank,
        row_number() OVER (
                PARTITION BY gamma
                ORDER BY log2_bf_tf DESC
            ) AS least_freq_rank,
    FROM
        {sdf.physical_name}
    WHERE
        value IS NOT NULL
    """

    return sql


def tf_chart_data(
    linker: Linker,
    tf_comparison_records: list[ComparisonLevelDetailedRecord],
    n_most_freq: int,
    n_least_freq: int,
    vals_to_include: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # we want a version of column_info_settings that is tied to duckdb, for local use
    # only need this for tf name
    column_info_settings = replace(
        linker._settings_obj.column_info_settings, sql_dialect="duckdb"
    )

    con = linker._db_api.duckdb_con
    levels_chart_data_sqls: list[str] = []
    for comparison_record in tf_comparison_records:
        # we know this is not None, as we have filtered out records without tf adjs
        tf_col_name: str = cast(str, comparison_record.tf_adjustment_column)
        sdf_tf_table = linker.table_management.compute_tf_table(tf_col_name)
        # register tf table in duckdb
        sdf_tf_table.as_duckdbpyrelation()
        input_column = InputColumn(
            tf_col_name,
            column_info_settings=column_info_settings,
            sqlglot_dialect_str="duckdb",
        )
        levels_chart_data_sql = comparison_level_to_tf_chart_data_sql(
            comparison_record, sdf_tf_table, input_column
        )
        levels_chart_data_sqls.append(levels_chart_data_sql)

    full_sql = join_sql_with_union_all(levels_chart_data_sqls)
    chart_data_table = con.sql(full_sql)

    df_table_name = (
        f"__splink__df_td_adjustment_chart_data_{n_least_freq}_{n_most_freq}"
    )
    con.register(df_table_name, chart_data_table)

    vals_not_included = (
        set(vals_to_include)
        - set(x[0] for x in chart_data_table.select("value").fetchall())
        if vals_to_include
        else {}
    )
    if vals_not_included:
        warnings.warn(
            f"Values {vals_not_included} from `vals_to_include` were not found in "
            f"the dataset so are not included in the chart.",
            stacklevel=2,
        )

    # Histogram data
    bin_width = 0.5  # Specify the desired bin width

    min_value = floor(min(x[0] for x in chart_data_table["log2_bf_final"].fetchall()))
    max_value = ceil(max(x[0] for x in chart_data_table["log2_bf_final"].fetchall()))
    bin_edges = list(
        map(
            lambda x: x / 2, range(min_value * 2, max_value * 2 + 1, int(2 * bin_width))
        )
    )

    histogram_data_table = con.sql(
        f"""
        WITH binned AS (
            SELECT
                gamma,
                log2_bf,
                unnest(map_entries(histogram(log2_bf_final, {bin_edges}))) AS hist_data,
                hist_data['key'] AS bin_upper,
                hist_data['value'] AS count,
                bin_upper - {bin_width/2} AS log2_bf_final,
                (bin_upper - {bin_width})::VARCHAR || '-' || (bin_upper)::VARCHAR
                    AS log2_bf_desc,
                -- have to repeat ourselves as duckdb can't disambiguate log2_bf_final
                -- in this relation as opposed to the base table
                bin_upper - {bin_width/2} - log2_bf AS log2_bf_tf
            FROM
                {df_table_name}
            GROUP BY
                gamma, log2_bf
        )
        -- only fields we need for the chart
        SELECT
            gamma,
            count,
            log2_bf_desc,
            log2_bf_tf,
            log2_bf_final,
        FROM
            binned
        WHERE
            count > 0
        """
    )

    # Filter values
    # important we do this after we have calculated histogram data
    chart_data_table = chart_data_table.filter(
        f"least_freq_rank < {n_least_freq} OR most_freq_rank < {n_most_freq}"
    )
    if vals_to_include:
        chart_data_table = chart_data_table.filter(
            f"""value IN ('{"', '".join(vals_to_include)}')"""
        )

    # trim down to only the data we need for the chart
    main_chart_data = record_dicts_from_relation(
        chart_data_table.select(
            "gamma", "value", "log2_bf_final", "log2_bf_tf", "log2_bf"
        )
    )
    tf_levels = [cl.comparison_vector_value for cl in tf_comparison_records]
    hist_data = record_dicts_from_relation(
        histogram_data_table.filter(f"gamma IN {tf_levels}")
    )

    # TODO: worth handling the case where we hit an error before and we don't drop?
    # don't expect long-lived processes with duckdb backend, so probably not crucial
    con.execute(f"DROP VIEW {df_table_name}")

    return main_chart_data, hist_data

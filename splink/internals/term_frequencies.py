from __future__ import annotations

# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107
import logging
import warnings
from functools import reduce
from math import ceil, floor
from typing import TYPE_CHECKING, Any, Optional

from splink.internals.charts import (
    ChartReturnType,
    altair_or_json,
    load_chart_definition,
)
from splink.internals.duckdb.duckdb_helpers.duckdb_helpers import (
    record_dicts_from_relation,
)
from splink.internals.input_column import InputColumn
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

    sql = f"""
    select
    {col_name}, cast(count(*) as float8) / (select
        count({col_name}) as total from {table_name})
            as {input_column.tf_name}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def _join_tf_to_df_concat_sql(linker: Linker) -> str:
    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns

    select_cols = []

    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        select_cols.append(f"{tbl}.{col.tf_name}")

    column_names_in_df_concat = linker._concat_table_column_names

    aliased_concat_column_names = [
        f"__splink__df_concat.{col} AS {col}" for col in column_names_in_df_concat
    ]

    select_cols = aliased_concat_column_names + select_cols
    select_cols_str = ", ".join(select_cols)

    templ = "left join {tbl} on __splink__df_concat.{col} = {tbl}.{col}"

    left_joins = []
    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        sql = templ.format(tbl=tbl, col=col.name)
        left_joins.append(sql)

    left_joins_str = " ".join(left_joins)

    sql = f"""
    select {select_cols_str}
    from __splink__df_concat
    {left_joins_str}
    """

    return sql


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
    tf_cols_not_already_populated = []

    input_table_columns = input_table.columns if input_table is not None else []

    for col in linker._settings_obj._term_frequency_columns:
        # Create an InputColumn for the tf column name to check membership
        tf_col_obj = linker._settings_obj._input_column(col.tf_name)
        if tf_col_obj not in input_table_columns:
            tf_cols_not_already_populated.append(col)

    cache = linker._intermediate_table_cache

    select_cols = [f"{input_tablename}.*"]

    for col in tf_cols_not_already_populated:
        tbl = colname_to_tf_tablename(col)
        if tbl in cache:
            select_cols.append(f"{tbl}.{col.tf_name}")

    template = "left join {tbl} on " + input_tablename + ".{col} = {tbl}.{col}"
    template_with_alias = (
        "left join ({subquery}) as {_as} on " + input_tablename + ".{col} = {_as}.{col}"
    )

    left_joins = []
    for i, col in enumerate(tf_cols_not_already_populated):
        tbl = colname_to_tf_tablename(col)
        if tbl in cache:
            sql = template.format(tbl=tbl, col=col.name)
            left_joins.append(sql)
        elif "__splink__df_concat_with_tf" in cache:
            subquery = f"""
            select distinct {col.name}, {col.tf_name}
            from __splink__df_concat_with_tf
            """
            _as = f"nodes_tf__{i}"
            sql = template_with_alias.format(subquery=subquery, col=col.name, _as=_as)
            select_cols.append(f"{_as}.{col.tf_name}")
            left_joins.append(sql)
        else:
            select_cols.append(f"null as {col.tf_name}")

    select_cols_str = ", ".join(select_cols)
    left_joins_str = "\n".join(left_joins)

    sql = f"""
    select {select_cols_str}
    from {input_tablename}
    {left_joins_str}

    """
    return sql


def compute_all_term_frequencies_sqls(
    linker: Linker, pipeline: CTEPipeline
) -> list[dict[str, str]]:
    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns

    if not tf_cols:
        return [
            {
                "sql": "select * from __splink__df_concat",
                "output_table_name": "__splink__df_concat_with_tf",
            }
        ]

    sqls = []
    cache = linker._intermediate_table_cache
    for tf_col in tf_cols:
        tf_table_name = colname_to_tf_tablename(tf_col)

        if tf_table_name in cache:
            tf_table = cache.get_with_logging(tf_table_name)
            pipeline.append_input_dataframe(tf_table)
        else:
            sql = term_frequencies_for_single_column_sql(tf_col)
            sql_info = {"sql": sql, "output_table_name": tf_table_name}
            sqls.append(sql_info)

    sql = _join_tf_to_df_concat_sql(linker)
    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_concat_with_tf",
    }
    sqls.append(sql_info)

    return sqls


def comparison_level_to_tf_chart_data(
    cl: dict[str, Any], con
) -> dict[str, Any]:
    sdf = cl["df_tf"]
    input_col = cl["input_col"]

    # ensure our duckdb connexion has access to this table
    sdf.as_duckdbpyrelation()

    sql = f"""
    SELECT
        {input_col.name} AS value,
        {input_col.tf_name} AS tf,
        {cl["u_probability"]}::DOUBLE AS u_probability,
        {cl["tf_adjustment_weight"]}::DOUBLE AS tf_adjustment_weight,
        -- TF match weight scaled by tf_adjustment_weight
        log2(u_probability/tf) * tf_adjustment_weight AS log2_bf_tf,
        -- Tidy up columns
        {cl["comparison_vector_value"]} AS gamma,
        '{input_col.unquote().name}' AS tf_col,
        {cl["log2_bayes_factor"]}::DOUBLE AS log2_bf,
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

    return con.sql(sql)


def tf_adjustment_chart(
    linker: Linker,
    col: str,
    n_most_freq: int,
    n_least_freq: int,
    vals_to_include: list[str],
    as_dict: bool,
) -> ChartReturnType:
    # Data for chart
    comparison = linker._settings_obj._get_comparison_by_output_column_name(col)
    tf_comparison_records = [
        detailed_rec
        for detailed_rec in comparison._as_detailed_records
        if detailed_rec.has_tf_adjustments
    ]

    keys_to_retain = [
        "comparison_vector_value",
        "label_for_charts",
        "tf_adjustment_column",
        "tf_adjustment_weight",
        "u_probability",
        "log2_bayes_factor",
    ]

    # Select levels with TF adjustments
    comparison_records = [
        {k: getattr(cl, k) for k in keys_to_retain} for cl in tf_comparison_records
    ]

    column_info_settings = linker._settings_obj.column_info_settings
    sqlglot_dialect_str = linker._settings_obj._sql_dialect_str

    # Add data ("df_tf") to each level
    comparison_records = [
        dict(
            cl,
            **{
                "df_tf": linker.table_management.compute_tf_table(
                    cl["tf_adjustment_column"]
                ),
                "input_col": InputColumn(
                    cl["tf_adjustment_column"],
                    column_info_settings=column_info_settings,
                    sqlglot_dialect_str=sqlglot_dialect_str,
                )

            },
        )
        for cl in comparison_records
    ]
    con = linker._db_api.duckdb_con

    levels_chart_data = [
        comparison_level_to_tf_chart_data(
            cl, con
        )
        for cl in comparison_records
    ]
    df = reduce(
        lambda relation_left, relation_right: relation_left.union(relation_right),
        levels_chart_data,
    )

    vals_not_included = (
        set(vals_to_include) - set(x[0] for x in df.select("value").fetchall())
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

    min_value = floor(min(x[0] for x in df["log2_bf_final"].fetchall()))
    max_value = ceil(max(x[0] for x in df["log2_bf_final"].fetchall()))
    bin_edges = list(
        map(
            lambda x: x / 2, range(min_value * 2, max_value * 2 + 1, int(2 * bin_width))
        )
    )

    df_table_name = (
        f"__splink__df_td_adjustment_chart_"
        f"data_{col}_{n_least_freq}_{n_most_freq}"
    )
    con.register(df_table_name, df)

    reln = con.sql(
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
    df = df.filter(
        f"least_freq_rank < {n_least_freq} OR most_freq_rank < {n_most_freq}"
    )
    if vals_to_include:
        df = df.filter(f"value IN ('{"', '".join(vals_to_include)}')")

    chart_path = "tf_adjustment_chart.json"
    chart = load_chart_definition(chart_path)

    # Complete chart schema
    tf_levels = [cl.comparison_vector_value for cl in tf_comparison_records]
    labels = [
        f"{cl.label_for_charts} (TF col: {cl.tf_adjustment_column})"
        for cl in tf_comparison_records
    ]

    # trim down to only the data we need for the chart
    main_chart_data = record_dicts_from_relation(
        df.select("gamma", "value", "log2_bf_final", "log2_bf_tf", "log2_bf")
    )
    hist_data = record_dicts_from_relation(reln.filter(f"gamma IN {tf_levels}"))

    # TODO: worth handling the case where we hit an error before and we don't drop?
    # don't expect long-lived processes with duckdb backend, so probably not crucial
    con.execute(f"DROP VIEW {df_table_name}")

    chart["datasets"]["data"] = main_chart_data
    chart["datasets"]["hist"] = hist_data
    chart["config"]["params"][0]["value"] = max(tf_levels)
    chart["config"]["params"][0]["bind"]["options"] = tf_levels
    chart["config"]["params"][0]["bind"]["labels"] = labels

    # filters = [
    #     f"datum.most_freq_rank < {n_most_freq}",
    #     f"datum.least_freq_rank < {n_least_freq}",
    #     " | ".join([f"datum.value == '{v}'" for v in vals_to_include])
    # ]
    # filter_text = " | ".join(filters)
    # chart["hconcat"][0]["layer"][0]["transform"][2]["filter"] = filter_text
    # chart["hconcat"][0]["layer"][2]["transform"][2]["filter"] = filter_text

    # PLACEHOLDER (until we work out adding a dynamic title based on the filtered data)
    chart["hconcat"][0]["layer"][0]["encoding"]["x"]["title"] = "TF column value"
    chart["hconcat"][0]["layer"][-1]["encoding"]["x"]["title"] = "TF column value"
    chart["hconcat"][0]["layer"][0]["encoding"]["tooltip"][0]["title"] = "Value"

    return altair_or_json(chart, as_dict=as_dict)

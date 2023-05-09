from __future__ import annotations

# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107
import logging
from typing import TYPE_CHECKING

from .input_column import InputColumn, remove_quotes_from_identifiers

from .charts import load_chart_definition, vegalite_or_json

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker

logger = logging.getLogger(__name__)


def colname_to_tf_tablename(input_column: InputColumn):
    input_col_no_quotes = remove_quotes_from_identifiers(
        input_column.input_name_as_tree
    )

    input_column = input_col_no_quotes.sql().replace(" ", "_")
    return f"__splink__df_tf_{input_column}"


def term_frequencies_for_single_column_sql(
    input_column: InputColumn, table_name="__splink__df_concat"
):
    col_name = input_column.name()

    sql = f"""
    select
    {col_name}, cast(count(*) as double) / (select
        count({col_name}) as total from {table_name})
            as {input_column.tf_name()}
    from {table_name}
    where {col_name} is not null
    group by {col_name}
    """

    return sql


def _join_tf_to_input_df_sql(linker: Linker):
    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns

    select_cols = []

    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        if tbl in linker._intermediate_table_cache:
            tbl = linker._intermediate_table_cache[tbl].physical_name
        tf_col = col.tf_name()
        select_cols.append(f"{tbl}.{tf_col}")

    select_cols.insert(0, "__splink__df_concat.*")
    select_cols = ", ".join(select_cols)

    templ = "left join {tbl} on __splink__df_concat.{col} = {tbl}.{col}"

    left_joins = []
    for col in tf_cols:
        tbl = colname_to_tf_tablename(col)
        if tbl in linker._intermediate_table_cache:
            tbl = linker._intermediate_table_cache[tbl].physical_name
        sql = templ.format(tbl=tbl, col=col.name())
        left_joins.append(sql)

    # left_joins = [
    #     templ.format(tbl=colname_to_tf_tablename(col), col=col.name())
    #     for col in tf_cols
    # ]
    left_joins = " ".join(left_joins)

    sql = f"""
    select {select_cols}
    from __splink__df_concat
    {left_joins}
    """

    return sql


def term_frequencies_from_concat_with_tf(input_column):
    sql = f"""
        select
        distinct {input_column.name()},
        {input_column.tf_name()}
        from __splink__df_concat_with_tf
    """

    return sql


def compute_all_term_frequencies_sqls(linker: Linker) -> list[dict]:
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
    for tf_col in tf_cols:
        tf_table_name = colname_to_tf_tablename(tf_col)

        if tf_table_name not in linker._intermediate_table_cache:
            sql = term_frequencies_for_single_column_sql(tf_col)
            sql = {"sql": sql, "output_table_name": tf_table_name}
            sqls.append(sql)

    sql = _join_tf_to_input_df_sql(linker)
    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_concat_with_tf",
    }
    sqls.append(sql)

    return sqls


def compute_term_frequencies_from_concat_with_tf(linker: "Linker"):
    """If __splink__df_concat_with_tf already exists in your database,
    reverse engineer the underlying tf tables.

    __splink__df_concat_with_tf is a cached table and often output by
    users to disk, for use at a later point in time. As a result, it
    often exists in the database or is easily accessible by the user,
    while the underlying tf tables are not.
    """

    settings_obj = linker._settings_obj
    tf_cols = settings_obj._term_frequency_columns
    cache = linker._intermediate_table_cache

    tf_table = []
    for tf_col in tf_cols:
        tf_table_name = colname_to_tf_tablename(tf_col)

        if tf_table_name not in cache:
            sql = term_frequencies_from_concat_with_tf(tf_col)
            sql = {
                "sql": sql,
                "output_table_name": colname_to_tf_tablename(tf_col),
            }
            tf_table.append(sql)
        else:
            tf_table.append(cache[tf_table_name])

    return tf_table


def tf_adjustment_chart(
    linker: Linker, col, n_most_freq, n_least_freq, vals_to_include, as_dict
):

    # Data for chart
    df_predict = [
        t for t in linker._names_of_tables_created_by_splink if "df_predict" in t
    ][0]

    df = linker.query_sql(
        f"""
        WITH tmp AS (
        select distinct
        gamma_{col} AS gamma,
        CASE WHEN tf_{col}_l >= tf_{col}_r THEN {col}_l ELSE {col}_r END AS value,
        log(bf_tf_adj_{col})/log(2) AS log2_bf_tf,
        log(bf_{col})/log(2) AS log2_bf
        from {df_predict}
    )
    SELECT *,
        row_number() over (partition by gamma order by log2_bf_tf desc) AS least_freq_rank,
        row_number() over (partition by gamma order by log2_bf_tf) AS most_freq_rank
    FROM tmp
    """
    )

    # Filter values
    selected = False if not vals_to_include else df["value"].isin(vals_to_include)
    least_freq = True if not n_least_freq else df["least_freq_rank"] <= n_least_freq
    most_freq = True if not n_most_freq else df["most_freq_rank"] <= n_most_freq
    mask = selected | least_freq | most_freq
    df = df[mask]

    # Select relevant comparison column and levels
    c = linker._settings_obj._get_comparison_by_output_column_name(col)
    cl = [
        l
        for l in c._as_detailed_records
        if l["has_tf_adjustments"] and l["tf_adjustment_column"] == col
    ]
    tf_levels = [str(l["comparison_vector_value"]) for l in cl]
    labels = [l["label_for_charts"] for l in cl]

    df = df[df["gamma"].astype("str").isin(tf_levels)].sort_values("least_freq_rank")

    chart_path = "tf_adjustment_chart.json"
    chart = load_chart_definition(chart_path)

    # Complete chart schema
    chart["data"]["values"] = df.to_dict("records")
    chart["layer"][0]["encoding"]["tooltip"][0]["title"] = col
    chart["layer"][0]["encoding"]["x"]["title"] = col
    chart["layer"][-1]["encoding"]["x"]["title"] = col
    chart["params"][0]["value"] = max(tf_levels)
    chart["params"][0]["bind"]["options"] = tf_levels
    chart["params"][0]["bind"]["labels"] = labels

    return vegalite_or_json(chart, as_dict=as_dict)

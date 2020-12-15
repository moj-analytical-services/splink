from copy import deepcopy
import math
import re

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False

_outer_chart_spec_freq = {
    "config": {"view": {"continuousWidth": 400, "continuousHeight": 300}},
    "vconcat": [],
    "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
}

_inner_chart_spec_freq = {
    "hconcat": [
        {
            "data": {"values": None},
            "mark": {"type": "line", "interpolate": "step-after"},
            "encoding": {
                "x": {
                    "type": "quantitative",
                    "field": "percentile_ex_nulls",
                    "sort": "descending",
                    "title": "Percentile",
                },
                "y": {
                    "type": "quantitative",
                    "field": "token_count",
                    "title": "Count of values",
                },
                "tooltip": [
                    {"field": "token_count", "type": "quantitative"},
                    {"field": "percentile_ex_nulls", "type": "quantitative"},
                    {"field": "percentile_inc_nulls", "type": "quantitative"},
                    {"field": "total_non_null_rows", "type": "quantitative"},
                    {"field": "total_rows_inc_nulls", "type": "quantitative"},
                    {"field": "proportion_of_non_null_rows", "type": "quantitative"},
                ],
            },
            "title": "Distribution of counts of values in column",
        },
        {
            "data": {"values": None},
            "mark": "bar",
            "encoding": {
                "x": {
                    "type": "nominal",
                    "field": "value",
                    "sort": "-y",
                    "title": None,
                },
                "y": {
                    "type": "quantitative",
                    "field": "token_count",
                    "title": "Value count",
                },
                "tooltip": [
                    {"field": "value", "type": "nominal"},
                    {"field": "token_count", "type": "quantitative"},
                    {"field": "total_non_null_rows", "type": "quantitative"},
                    {"field": "total_rows_inc_nulls", "type": "quantitative"},
                ],
            },
            "title": "Top 20 values by value count",
        },
    ]
}


def _non_array_group(
    cols_or_exprs, table_name, top_n_limit=None, group_sort_order="desc"
):
    """
    Generate a sql expression that will yield a table with value counts grouped by the provided
    column expression e.g. "dmetaphone(name)" or combination of column expression
    (e.g. ["dmetaphone(first_name)" , "dmetaphone(surname)"]..

    Where an array is provided, value counts will be grouped by the concatenation of
    all the column expressions


    """
    # If user has provided a string, make it into a list of length one for convenience
    if type(cols_or_exprs) == str:
        cols_or_exprs = [cols_or_exprs]

    # When considering multiple columns,
    # Need an expression which returns null if any part is null else returns concat_ws

    # This is more relevant for our use case e.g. the blocking
    # Since comparisons are only created where blocking values are non null
    null_exprs = [f"{c} is null" for c in cols_or_exprs]
    null_exprs = " OR ".join(null_exprs)
    ws_cols = ", ".join(cols_or_exprs)

    # This is the expression with the correct property
    case_expr = f"""
    case when
    {null_exprs} then null
    else
    concat_ws(' | ', {ws_cols})
    end
    """

    # Group name will be the value which will identify this group in the final table
    group_name = "_".join(cols_or_exprs)
    group_name = re.sub("[^0-9a-zA-Z_]", " ", group_name)
    group_name = re.sub("\s+", " ", group_name)

    limit_expr = ""
    if top_n_limit:
        limit_expr = f"limit {top_n_limit}"

    sql = f"""
    (
    select
        count(*) as value_count,
        {case_expr} as value,
        "{group_name}" as group_name,
        (select count({case_expr}) from {table_name}) as total_non_null_rows,
        (select count(*) from {table_name}) as total_rows_inc_nulls
    from {table_name}
    where {case_expr} is not null
    group by {case_expr}
    order by group_name, count(*) {group_sort_order}
    {limit_expr}
    )
    """

    return sql


def _generate_df_all_column_value_frequencies(
    list_of_col_exprs, df, spark, top_n_limit=None
):
    """
    Each element in list_of_col_exprs can be a list or a string
        If string, the contents should be a sql expression which
        is used in the group by to find value frequencies

        If a list, each item in the list is a sql expression which
        are then concatenated using concat_ws, and this is used
        in the gropu by to find value frequencies

    Example:
    [
    'first_name',
    ['first_name', 'surname'],
    ['dmetaphone(first_name), dmetaphone(surname)']
    ]
    """

    df.createOrReplaceTempView("df")
    to_union = [get_group_by_sql(c, "df", top_n_limit) for c in list_of_cols_or_exprs]
    sql = "\n union all \n".join(to_union)
    return spark.sql(sql)


def _get_df_percentiles(df_unioned_freqs, spark):
    """Take df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile cahrt
    """

    df_unioned_freqs.createOrReplaceTempView("df_unioned_freqs")

    sql = """
    select sum(token_count) as sum_tokens_in_token_count_group, token_count, group_name,
    first(total_non_null_rows) as total_non_null_rows,
    first(total_rows_inc_nulls) as total_rows_inc_nulls
    from df_unioned_freqs
    group by group_name, token_count
    order by group_name, token_count desc
    """
    df_total_in_token_counts = spark.sql(sql)
    df_total_in_token_counts.createOrReplaceTempView("df_total_in_token_counts")

    sql = """
    select sum(sum_tokens_in_token_count_group) over (partition by group_name order by token_count desc) as token_count_cumsum,
    sum_tokens_in_token_count_group, token_count, group_name,  total_non_null_rows, total_rows_inc_nulls
    from df_total_in_token_counts


    """
    df_total_in_token_counts_cumulative = spark.sql(sql)
    df_total_in_token_counts_cumulative.createOrReplaceTempView(
        "df_total_in_token_counts_cumulative"
    )

    sql = """
    select
    1 - (token_count_cumsum/total_non_null_rows) as percentile_ex_nulls,
    1 - (token_count_cumsum/total_rows_inc_nulls) as percentile_inc_nulls,

    token_count, group_name, total_non_null_rows, total_rows_inc_nulls,
    sum_tokens_in_token_count_group
    from df_total_in_token_counts_cumulative

    """
    df_percentiles = spark.sql(sql)
    return df_percentiles


def _get_df_top_n(limit):
    """Take df_all_column_value_frequencies and
    use limit statements to take only the top n values
    """


def _collect_and_group_percentiles_df(df_percentiles):
    """Turn df_percentiles into a grouped dictionary
    containing the data need for our charts
    """

    percentiles_groups = {}
    percentiles_rows = [r.asDict() for r in df_percentiles.collect()]
    for r in percentiles_rows:

        if r["group_name"] not in percentiles_groups:

            percentiles_groups[r["group_name"]] = []

            # If we do not already have a row for 100th percentile
            if r["percentile_ex_nulls"] != 1.0:
                first_row = {
                    "percentile_inc_nulls": 1.0,
                    "percentile_ex_nulls": 1.0,
                    "token_count": r["token_count"],
                }
                percentiles_groups[r["group_name"]].append(first_row)

        percentiles_groups[r["group_name"]].append(r)
        del r["group_name"]
        r["proportion_of_non_null_rows"] = (
            r["sum_tokens_in_token_count_group"] / r["total_non_null_rows"]
        )

    return percentiles_groups


def column_value_frequencies_chart(list_of_columns):
    pass


def column_combination_value_frequencies_chart(list_of_col_combinations):
    pass


def column_value_frequencies_chart(col_or_col_list):
    pass

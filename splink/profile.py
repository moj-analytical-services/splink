import re
from copy import deepcopy

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from functools import reduce


altair_installed = True
try:
    import altair as alt
    from altair import Chart
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
                    "field": "value_count",
                    "title": "Count of values",
                },
                "tooltip": [
                    {"field": "value_count", "type": "quantitative"},
                    {"field": "percentile_ex_nulls", "type": "quantitative"},
                    {"field": "percentile_inc_nulls", "type": "quantitative"},
                    {"field": "total_non_null_rows", "type": "quantitative"},
                    {"field": "total_rows_inc_nulls", "type": "quantitative"},
                    {"field": "proportion_of_non_null_rows", "type": "quantitative"},
                ],
            },
            "title": {
                "text": "Distribution of counts of values in column",
                "subtitle": "Subtitle Text",
            },
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
                    "field": "value_count",
                    "title": "Value count",
                },
                "tooltip": [
                    {"field": "value", "type": "nominal"},
                    {"field": "value_count", "type": "quantitative"},
                    {"field": "total_non_null_rows", "type": "quantitative"},
                    {"field": "total_rows_inc_nulls", "type": "quantitative"},
                ],
            },
            "title": "Top 20 values by value count",
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
                    "field": "value_count",
                    "title": "Value count",
                },
                "tooltip": [
                    {"field": "value", "type": "nominal"},
                    {"field": "value_count", "type": "quantitative"},
                    {"field": "total_non_null_rows", "type": "quantitative"},
                    {"field": "total_rows_inc_nulls", "type": "quantitative"},
                ],
            },
            "title": "Bottom 20 values by value count",
        },
    ]
}


def _get_inner_chart_spec_freq(percentile_data, top_n_data, bottom_n_data, col_name):

    inner_spec = deepcopy(_inner_chart_spec_freq)

    total_rows_inc_nulls = percentile_data[0]["total_rows_inc_nulls"]
    total_non_null_rows = percentile_data[0]["total_non_null_rows"]
    perc = total_non_null_rows / total_rows_inc_nulls

    sub = (
        f"Of the {total_rows_inc_nulls} rows in the df, "
        f"{total_non_null_rows} ({1-perc:,.1%}) are null for this column"
    )
    sub = sub.format(**percentile_data[0])
    inner_spec["hconcat"][0]["data"]["values"] = percentile_data
    inner_spec["hconcat"][0]["title"][
        "text"
    ] = f"Distribution of counts of values in column {col_name}"

    inner_spec["hconcat"][0]["title"]["subtitle"] = sub

    inner_spec["hconcat"][1]["data"]["values"] = top_n_data
    inner_spec["hconcat"][1]["title"] = f"Top {len(top_n_data)} values by value count"

    inner_spec["hconcat"][2]["data"]["values"] = bottom_n_data
    inner_spec["hconcat"][2][
        "title"
    ] = f"Bottom {len(bottom_n_data)} values by value count"

    max_val = top_n_data[0]["value_count"]
    inner_spec["hconcat"][2]["encoding"]["y"]["scale"] = {"domain": [0, max_val]}

    return inner_spec


def _group_name(cols_or_exprs):
    group_name = "_".join(cols_or_exprs)
    group_name = re.sub(r"[^0-9a-zA-Z_\(\),]", " ", group_name)
    group_name = re.sub(r"\s+", " ", group_name)
    return group_name


def _non_array_group(cols_or_exprs, table_name):
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
    concat_ws(', ', {ws_cols})
    end
    """

    # Group name will be the value which will identify this group in the final table
    group_name = _group_name(cols_or_exprs)

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
    order by group_name, count(*) desc
    )
    """

    return sql


def _array_group(col, df, spark):
    """
    Generate a sql expression that will yield a table with value counts grouped by the provided
    column expression e.g. "dmetaphone(name)" or combination of column expression
    (e.g. ["dmetaphone(first_name)" , "dmetaphone(surname)"]..

    Where an array is provided, value counts will be grouped by the concatenation of
    all the column expressions


    """

    # Group name will be the value which will identify this group in the final table
    group_name = _group_name([col])
    df.createOrReplaceTempView("df")
    sql = f"""
    with df_exp as (
    select
        explode(ifnull({col}, array(null))) as value,
        "{group_name}" as group_name
    from df
    )

    select
        count(*) as value_count,
        value,
        group_name,
        (select count(value) from df_exp) as total_non_null_rows,
        (select count(*) from df_exp) as total_rows_inc_nulls
    from df_exp
    where value is not null
    group by value, group_name
    order by group_name, count(*) desc
    """

    return spark.sql(sql)


def _generate_df_all_column_value_frequencies(list_of_col_exprs, df, spark):
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

    to_union = [_non_array_group(c, "df") for c in list_of_col_exprs]
    sql = "\n union all \n".join(to_union)
    return spark.sql(sql)


def _generate_df_all_column_value_frequencies_array(list_of_array_cols, df, spark):

    to_union = [_array_group(c, df, spark) for c in list_of_array_cols]
    df = reduce(DataFrame.unionAll, to_union)
    return df


def _get_df_percentiles(df_all_column_value_frequencies, spark):
    """Take df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile cahrt
    """

    df_all_column_value_frequencies.createOrReplaceTempView(
        "df_all_column_value_frequencies"
    )

    sql = """
    select sum(value_count) as sum_tokens_in_value_count_group, value_count, group_name,
    first(total_non_null_rows) as total_non_null_rows,
    first(total_rows_inc_nulls) as total_rows_inc_nulls
    from df_all_column_value_frequencies
    group by group_name, value_count
    order by group_name, value_count desc
    """
    df_total_in_value_counts = spark.sql(sql)
    df_total_in_value_counts.createOrReplaceTempView("df_total_in_value_counts")

    sql = """
    select sum(sum_tokens_in_value_count_group) over (partition by group_name order by value_count desc) as value_count_cumsum,
    sum_tokens_in_value_count_group, value_count, group_name,  total_non_null_rows, total_rows_inc_nulls
    from df_total_in_value_counts


    """
    df_total_in_value_counts_cumulative = spark.sql(sql)
    df_total_in_value_counts_cumulative.createOrReplaceTempView(
        "df_total_in_value_counts_cumulative"
    )

    sql = """
    select
    1 - (value_count_cumsum/total_non_null_rows) as percentile_ex_nulls,
    1 - (value_count_cumsum/total_rows_inc_nulls) as percentile_inc_nulls,

    value_count, group_name, total_non_null_rows, total_rows_inc_nulls,
    sum_tokens_in_value_count_group
    from df_total_in_value_counts_cumulative

    """
    df_percentiles = spark.sql(sql)
    return df_percentiles


def _get_df_top_bottom_n(
    df_all_column_value_frequencies, spark, limit=20, value_order="desc"
):
    """Take df_all_column_value_frequencies and
    use limit statements to take only the top n values
    """
    df_all_column_value_frequencies.createOrReplaceTempView(
        "df_all_column_value_frequencies"
    )

    sql = """
    select distinct group_name
    from df_all_column_value_frequencies
    """
    group_names = [r.group_name for r in spark.sql(sql).collect()]

    sql = """
    (select *
    from df_all_column_value_frequencies
    where group_name = '{group_name}'
    order by value_count {value_order}
    limit {limit})
    """

    to_union = [
        sql.format(group_name=g, limit=limit, value_order=value_order)
        for g in group_names
    ]

    sql = "\n union all \n".join(to_union)

    df = spark.sql(sql)

    return df


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
                first_row = deepcopy(r)
                first_row["percentile_inc_nulls"] = 1.0
                first_row["percentile_ex_nulls"] = 1.0

                percentiles_groups[r["group_name"]].append(first_row)

        percentiles_groups[r["group_name"]].append(r)
        del r["group_name"]
        r["proportion_of_non_null_rows"] = (
            r["sum_tokens_in_value_count_group"] / r["total_non_null_rows"]
        )

    return percentiles_groups


def _collect_and_group_top_values(df_top):
    top_n_groups = {}
    top_n_rows = [r.asDict() for r in df_top.collect()]
    for r in top_n_rows:
        if r["group_name"] not in top_n_groups:
            top_n_groups[r["group_name"]] = []

        top_n_groups[r["group_name"]].append(r)
        del r["group_name"]

    return top_n_groups


def column_value_frequencies_chart(list_of_columns, df, spark, top_n=20, bottom_n=10):
    """Produce value frequency charts for the provided list of column names

    Args:
        list_of_col_combinations (list): A list of column names
        df (DataFrame): Dataframe to profile
        spark (SparkSession): SparkSession object
        top_n (int, optional): Number of values with the highest frequencies to display. Defaults to 20.
        bottom_n (int, optional): Number of values with the lowest frequencies to display. Defaults to 10.

    Returns:
        Chart: If Altair is installed, return a chart. If not,  then it returns the
        vega lite chart spec as a dictionary
    """
    return column_combination_value_frequencies_chart(
        list_of_columns, df, spark, top_n, bottom_n
    )


def column_combination_value_frequencies_chart(
    list_of_col_combinations, df, spark, top_n=20, bottom_n=10
):
    """Produce value frequency charts for the provided list of column names, expressions, or
    combinations thereof.

    Each element in list_of_col_combinations can be a list or a string:
        If string, the contents should be a sql expression which
        is used in the group by to find value frequencies

        If a list, each item in the list is a sql expression which
        are then concatenated using concat_ws, and this is used
        in the gropu by to find value frequencies

    Example:
    list_of_col_combinations = [
        'first_name',
        ['first_name', 'surname'],
        ['dmetaphone(first_name), dmetaphone(surname)']
    ]

    Args:
        list_of_col_combinations (list): A list of column names, expressions or
          combinations thereof for columns of array type
        df (DataFrame): Dataframe to profile
        spark (SparkSession): SparkSession object
        top_n (int, optional): Number of values with the highest frequencies to display. Defaults to 20.
        bottom_n (int, optional): Number of values with the lowest frequencies to display. Defaults to 10.

    Returns:
        Chart: If Altair is installed, return a chart. If not,  then it returns the
        vega lite chart spec as a dictionary
    """
    df_acvf = _generate_df_all_column_value_frequencies(
        list_of_col_combinations, df, spark
    )
    df_acvf.persist()

    df_perc = _get_df_percentiles(df_acvf, spark)
    df_top_n = _get_df_top_bottom_n(df_acvf, spark, top_n)
    df_bottom_n = _get_df_top_bottom_n(df_acvf, spark, bottom_n, value_order="asc")

    df_perc_collected = _collect_and_group_percentiles_df(df_perc)
    df_top_n_collected = _collect_and_group_top_values(df_top_n)
    df_bottom_n_collected = _collect_and_group_top_values(df_bottom_n)

    inner_charts = []
    for col_name in df_top_n_collected.keys():
        top_n_rows = df_top_n_collected[col_name]
        bottom_n_rows = df_bottom_n_collected[col_name]

        percentile_rows = df_perc_collected[col_name]
        inner_chart = _get_inner_chart_spec_freq(
            percentile_rows, top_n_rows, bottom_n_rows, col_name
        )
        inner_charts.append(inner_chart)

    outer_spec = deepcopy(_outer_chart_spec_freq)

    outer_spec["vconcat"] = inner_charts

    if altair_installed:
        return alt.Chart.from_dict(outer_spec)
    else:
        return outer_spec


def array_column_value_frequencies_chart(
    list_of_array_cols: list, df: DataFrame, spark: SparkSession, top_n=20, bottom_n=10
):
    """Produce value frequency charts for the provided list of columns names

    Args:
        list_of_array_cols (list): A list of column names for columns of array type
        df (DataFrame): Dataframe to profile
        spark (SparkSession): SparkSession object
        top_n (int, optional): Number of values with the highest frequencies to display. Defaults to 20.
        bottom_n (int, optional): Number of values with the lowest frequencies to display. Defaults to 10.

    Returns:
        Chart: If Altair is installed, return a chart. If not,  then it returns the
        vega lite chart spec as a dictionary
    """
    df_acvf = _generate_df_all_column_value_frequencies_array(
        list_of_array_cols, df, spark
    )
    df_acvf.persist()

    df_perc = _get_df_percentiles(df_acvf, spark)
    df_top_n = _get_df_top_bottom_n(df_acvf, spark, top_n)
    df_bottom_n = _get_df_top_bottom_n(df_acvf, spark, bottom_n, value_order="asc")

    df_perc_collected = _collect_and_group_percentiles_df(df_perc)
    df_top_n_collected = _collect_and_group_top_values(df_top_n)
    df_bottom_n_collected = _collect_and_group_top_values(df_bottom_n)

    inner_charts = []
    for col_name in df_top_n_collected.keys():
        top_n_rows = df_top_n_collected[col_name]
        bottom_n_rows = df_bottom_n_collected[col_name]
        percentile_rows = df_perc_collected[col_name]
        inner_chart = _get_inner_chart_spec_freq(
            percentile_rows, top_n_rows, bottom_n_rows, col_name
        )
        inner_charts.append(inner_chart)

    outer_spec = deepcopy(_outer_chart_spec_freq)

    outer_spec["vconcat"] = inner_charts

    if altair_installed:
        return alt.Chart.from_dict(outer_spec)
    else:
        return outer_spec


def _parse_blocking_rule(rule):
    rule = rule.lower()
    rule = rule.strip()
    rule = re.sub(r"\s+", " ", rule)
    rule = re.sub(r"\s+", " ", rule)

    cols = rule.split(" and ")

    cols = [[cc.strip() for cc in c.split("=")] for c in cols]

    # check symmetry, return none if not symmetric
    for c in cols:
        left = c[0].replace("r.", "l.")
        right = c[1].replace("r.", "l.")
        if left != right:
            return None

    cols = [c[0].replace("r.", "").replace("l.", "") for c in cols]

    return cols


def blocking_rules_to_column_combinations(blocking_rules: list):
    """Convert blocking rules as specified in a Splink settings dictionary
    into a list of column combintions, the format needed to input into
    column_combination_value_frequencies_chart

    Args:
        blocking_rules (list): A list of blocking rules as specified in a Splink
            settings dictionary

    Returns:
        list: list of column combinations
    """

    column_combinations = [_parse_blocking_rule(r) for r in blocking_rules]
    column_combinations = [c for c in column_combinations if c is not None]
    return column_combinations


def value_frequencies_chart_from_blocking_rules(
    blocking_rules: list, df: DataFrame, spark: SparkSession, top_n=20, bottom_n=10
):
    """Produce value frequency charts for the provided blocking rules

    Args:
        blocking_rules (list): A list of blocking rules as specified in a Splink
            settings dictionary
        df (DataFrame): Dataframe to profile
        spark (SparkSession): SparkSession object
        top_n (int, optional): Number of values with the highest frequencies to display. Defaults to 20.
        bottom_n (int, optional): Number of values with the lowest frequencies to display. Defaults to 10.

    Returns:
        Chart: If Altair is installed, return a chart. If not,  then it returns the
        vega lite chart spec as a dictionary
    """
    col_combinations = blocking_rules_to_column_combinations(blocking_rules)
    return column_combination_value_frequencies_chart(
        col_combinations, df, spark, top_n, bottom_n
    )

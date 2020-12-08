from copy import deepcopy
import math

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
            "mark": "line",
            "encoding": {
                "x": {
                    "type": "quantitative",
                    "field": "percentile",
                    "sort": "descending",
                    "title": "Percentile",
                },
                "y": {
                    "type": "quantitative",
                    "field": "token_count",
                    "title": "Count of values",
                },
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
                    "field": "count",
                    "title": "Value count",
                },
            },
            "title": "Top 20 values by value count",
        },
    ]
}


def _get_df_freq(df, col_name, spark, explode_arrays=True):

    data_types = dict(df.dtypes)
    df.createOrReplaceTempView("df")

    if data_types[col_name].startswith("array"):
        if explode_arrays:
            sql = f"""
            select explode({col_name}) as {col_name}
            from df
            """
            df = spark.sql(sql)
            df.createOrReplaceTempView("df")
        else:
            sql = f"""
            select concat_ws(', ', {col_name}) as {col_name}
            from df
            where concat_ws(', ', {col_name}) != ''
            """
            df = spark.sql(sql)
            df.createOrReplaceTempView("df")

    sql = f"""
    select {col_name} as value, count({col_name}) as count
    from df
    group by {col_name}
    order by count({col_name}) desc

    """
    return spark.sql(sql)


def _get_percentiles(df_freq, col_name):
    smallest_increment = 1 / df_freq.count()

    first_few_percentiles = [smallest_increment * i for i in range(21)]
    start_value = first_few_percentiles.pop()
    next_few_percentiles = [5 * smallest_increment * i + start_value for i in range(21)]
    percentiles = first_few_percentiles + next_few_percentiles
    start_value = percentiles[-1]

    start_range = math.ceil(start_value * 100)

    final_percentiles = [i / 100 for i in range(start_range, 101)]
    percentiles = percentiles + final_percentiles

    percentiles = [p for p in percentiles if p >= 0.0 and p <= 1.0]
    percentiles = [1 - p for p in percentiles]
    percentiles.sort()

    counts_at_percentiles = df_freq.approxQuantile("count", percentiles, 0.0)

    rows = []
    for i in range(len(percentiles)):
        row = {
            "percentile": percentiles[i],
            "token_count": counts_at_percentiles[i],
            "col_name": col_name,
        }
        rows.append(row)
    return rows


def _get_top_n(df_freq, col_name, n=30):
    df_top = df_freq.limit(n)
    rows_list = [r.asDict() for r in df_top.collect()]
    for r in rows_list:
        r["col_name"] = col_name
    return rows_list


def _get_inner_chart_spec_freq(percentile_data, top_n_data, col_name):

    inner_spec = deepcopy(_inner_chart_spec_freq)
    inner_spec["hconcat"][0]["data"]["values"] = percentile_data
    inner_spec["hconcat"][0][
        "title"
    ] = f"Distribution of counts of values in column {col_name}"
    inner_spec["hconcat"][1]["data"]["values"] = top_n_data
    inner_spec["hconcat"][1]["title"] = f"Top {len(top_n_data)} values by value count"

    return inner_spec


def freq_skew_chart(
    cols: list,
    df: DataFrame,
    spark: SparkSession,
    top_n: int = 30,
    explode_arrays: bool = True,
):
    """Create a chart of the frequency distribution of values in the given cols

    Args:
        cols (list): A list of columns to profile, e.g. ['first_name', 'surname']
        df (DataFrame): A dataframe containing the data to profile, must contain columns
            as specified in cols
        spark (SparkSession): SparkSession object
        top_n (int, optional): The number of most frequently occurring values to include
            in the charts. Defaults to 30.
        explode_arrays (bool, optional): Where array columns are specified, whether to
            explode them.  When False, `concat_ws` is used . Defaults to True.

    Returns:
        if Altair is installed returns a plot of value frequencies. if not,
        returns the vega lite chart spec as a dictionary
    """

    inner_charts = []
    for col_name in cols:
        df_freq = _get_df_freq(df, col_name, spark, explode_arrays=explode_arrays)
        percentile_rows = _get_percentiles(df_freq, col_name)
        top_n_rows = _get_top_n(df_freq, col_name, n=top_n)
        inner_chart = _get_inner_chart_spec_freq(percentile_rows, top_n_rows, col_name)
        inner_charts.append(inner_chart)

    outer_spec = deepcopy(_outer_chart_spec_freq)

    outer_spec["vconcat"] = inner_charts

    if altair_installed:
        return alt.Chart.from_dict(outer_spec)
    else:
        return outer_spec

from copy import deepcopy

_chart_spec = {
    "config": {"view": {"continuousWidth": 400, "continuousHeight": 300}},
    "vconcat": [],
    "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
}

_inner_chart_spec = {
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


def _get_df_freq(df, col_name, spark):
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


def _get_inner_chart_spec(percentile_data, top_n_data, col_name):

    iner = deepcopy(_chart_spec)


def freq_skew_chart(cols, df):

    inner_charts = []
    for col_name in cols:
        df_freq = _get_df_freq(df, col_name)
        percentile_rows = _get_percentiles(df_freq, col_name)
        top_n_rows = _get_top_n(df_freq, col_name)
        inner_chart = _get_inner_chart_spec(percentile_rows, top_n_rows, col_name)
        inner_charts.append(inner_chart)

    return


freq_skew_chart(["first_name"], df)

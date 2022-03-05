import re
from copy import deepcopy
from .charts import vegalite_or_json


def _group_name(cols_or_exprs):
    group_name = "_".join(cols_or_exprs)
    group_name = re.sub(r"[^0-9a-zA-Z_\(\),]", " ", group_name)
    group_name = re.sub(r"\s+", " ", group_name)
    return group_name


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
    distinct_value_count = percentile_data[0]["distinct_value_count"]
    perc = total_non_null_rows / total_rows_inc_nulls

    sub = (
        f"In this col, {total_rows_inc_nulls*(1-perc):,.0f} values ({1-perc:,.1%}) are null and there are "
        f"{distinct_value_count} distinct values"
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


def _get_df_percentiles():
    """Take df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile cahrt
    """

    sqls = []

    sql = """
    select sum(value_count) as sum_tokens_in_value_count_group,
    value_count,
    first(total_non_null_rows) as total_non_null_rows,
    first(total_rows_inc_nulls) as total_rows_inc_nulls,
    first(distinct_value_count) as distinct_value_count
    from df_all_column_value_frequencies
    group by value_count
    order by value_count desc
    """

    sqls.append({"sql": sql, "output_table_name": "df_total_in_value_counts"})

    sql = """
    select sum(sum_tokens_in_value_count_group) over (order by value_count desc) as value_count_cumsum,
    sum_tokens_in_value_count_group, value_count,   total_non_null_rows, total_rows_inc_nulls, distinct_value_count
    from df_total_in_value_counts
    """
    sqls.append(
        {"sql": sql, "output_table_name": "df_total_in_value_counts_cumulative"}
    )

    sql = """
    select
    1 - (cast(value_count_cumsum as float)/total_non_null_rows) as percentile_ex_nulls,
    1 - (cast(value_count_cumsum as float)/total_rows_inc_nulls) as percentile_inc_nulls,

    value_count,  total_non_null_rows, total_rows_inc_nulls,
    sum_tokens_in_value_count_group, distinct_value_count
    from df_total_in_value_counts_cumulative

    """
    sqls.append({"sql": sql, "output_table_name": "df_percentiles"})
    return sqls


def _get_df_top_bottom_n(limit=20, value_order="desc"):

    sql = f"""
    select *
    from df_all_column_value_frequencies

    order by value_count {value_order}
    limit {limit}
    """

    return sql


def _col_or_expr_frequencies_raw_data_sql(col_or_expr, table_name):

    sql = f"""

    select
        count(*) as value_count,
        {col_or_expr} as value,
        (select count({col_or_expr}) from {table_name}) as total_non_null_rows,
        (select count(*) from {table_name}) as total_rows_inc_nulls,
        (select count(distinct {col_or_expr}) from {table_name}) as distinct_value_count

    from {table_name}
    where {col_or_expr} is not null
    group by {col_or_expr}
    order by count(*) desc

    """

    return sql


def _add_100_percentile_to_df_percentiles(percentile_rows):

    r = percentile_rows[0]
    if r["percentile_ex_nulls"] != 1.0:
        first_row = deepcopy(r)
        first_row["percentile_inc_nulls"] = 1.0
        first_row["percentile_ex_nulls"] = 1.0

    percentile_rows.append(first_row)
    return percentile_rows


def column_frequency_chart(
    expression, linker, table_name="__splink__df_concat_with_tf"
):

    linker._initialise_df_concat_with_tf()

    sql = _col_or_expr_frequencies_raw_data_sql(
        expression, "__splink__df_concat_with_tf"
    )

    linker.enqueue_sql(sql, "df_all_column_value_frequencies")
    df_raw = linker.execute_sql_pipeline(materialise_as_hash=True)

    sqls = _get_df_percentiles()
    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    df_percentiles = linker.execute_sql_pipeline([df_raw])
    percentile_rows = df_percentiles.as_record_dict()
    percentile_rows = _add_100_percentile_to_df_percentiles(percentile_rows)

    sql = _get_df_top_bottom_n(10, "desc")
    linker.enqueue_sql(sql, "df_top_n")
    df_top_n = linker.execute_sql_pipeline([df_raw])
    top_n_rows = df_top_n.as_record_dict()

    sql = _get_df_top_bottom_n(10, "asc")
    linker.enqueue_sql(sql, "df_top_n")
    df_bottom_n = linker.execute_sql_pipeline([df_raw])
    bottom_n_rows = df_bottom_n.as_record_dict()

    inner_chart = _get_inner_chart_spec_freq(
        percentile_rows, top_n_rows, bottom_n_rows, expression
    )
    outer_spec = deepcopy(_outer_chart_spec_freq)
    outer_spec["vconcat"] = [inner_chart]

    return vegalite_or_json(outer_spec)

import logging
import re
from copy import deepcopy

from .charts import altair_or_json, load_chart_definition
from .misc import ensure_is_list

logger = logging.getLogger(__name__)


def _group_name(cols_or_expr):
    cols_or_expr = re.sub(r"[^0-9a-zA-Z_]", " ", cols_or_expr)
    cols_or_expr = re.sub(r"\s+", "_", cols_or_expr)
    return cols_or_expr


def expressions_to_sql(expressions):
    e = []
    for expr in expressions:
        if isinstance(expr, list):
            expr = ", ' ', ".join(expr)
            expr = f"concat({expr})"
        e.append(expr)

    return e


_outer_chart_spec_freq = {
    "config": {"view": {"continuousWidth": 400, "continuousHeight": 300}},
    "vconcat": [],
    "$schema": "https://vega.github.io/schema/vega-lite/v5.9.3.json",
}

chart_path = "profile_data.json"
_inner_chart_spec_freq = load_chart_definition(chart_path)


def _get_inner_chart_spec_freq(percentile_data, top_n_data, bottom_n_data, col_name):
    inner_spec = deepcopy(_inner_chart_spec_freq)

    total_rows_inc_nulls = percentile_data[0]["total_rows_inc_nulls"]
    total_non_null_rows = percentile_data[0]["total_non_null_rows"]
    distinct_value_count = percentile_data[0]["distinct_value_count"]
    perc = total_non_null_rows / total_rows_inc_nulls

    sub = (
        f"In this col, {total_rows_inc_nulls*(1-perc):,.0f} values "
        f"({1-perc:,.1%}) are null and there are "
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
    """Take __splink__df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile cahrt
    """

    sqls = []

    sql = """
    select sum(value_count) as sum_tokens_in_value_count_group,
    value_count,
    group_name,
    max(total_non_null_rows) as total_non_null_rows,
    max(total_rows_inc_nulls) as total_rows_inc_nulls,
    max(distinct_value_count) as distinct_value_count
    from __splink__df_all_column_value_frequencies
    group by group_name, value_count
    order by group_name, value_count desc
    """

    sqls.append({"sql": sql, "output_table_name": "df_total_in_value_counts"})

    sql = """
    select sum(sum_tokens_in_value_count_group)
        over (partition by group_name order by value_count desc) as value_count_cumsum,
    sum_tokens_in_value_count_group,
    value_count,
    group_name,
    total_non_null_rows,
    total_rows_inc_nulls,
    distinct_value_count
    from df_total_in_value_counts
    """
    sqls.append(
        {"sql": sql, "output_table_name": "df_total_in_value_counts_cumulative"}
    )

    sql = """
    select
    1 - (cast(value_count_cumsum as float)/total_non_null_rows)
        as percentile_ex_nulls,
    1 - (cast(value_count_cumsum as float)/total_rows_inc_nulls)
        as percentile_inc_nulls,
    value_count, group_name, total_non_null_rows, total_rows_inc_nulls,
    sum_tokens_in_value_count_group, distinct_value_count
    from df_total_in_value_counts_cumulative
    """
    sqls.append({"sql": sql, "output_table_name": "__splink__df_percentiles"})
    return sqls


def _get_df_top_bottom_n(expressions, limit=20, value_order="desc"):
    sql = """
    select * from
    (select *
    from __splink__df_all_column_value_frequencies
    where group_name = '{gn}'
    order by value_count {value_order}
    limit {limit}) top_bottom_freqs
    """

    to_union = [
        sql.format(gn=_group_name(g), limit=limit, value_order=value_order)
        for g in expressions
    ]

    sql = " union all ".join(to_union)

    return sql


def _col_or_expr_frequencies_raw_data_sql(cols_or_exprs, table_name):
    cols_or_exprs = ensure_is_list(cols_or_exprs)
    column_expressions = expressions_to_sql(cols_or_exprs)
    sqls = []
    for col_or_expr, raw_expr in zip(column_expressions, cols_or_exprs):
        gn = _group_name(col_or_expr)

        # If the supplied column string is a list of columns to be concatenated,
        # add a quick clause to filter out any instances whereby either column contains
        # a null value.
        if isinstance(raw_expr, list):
            null_exprs = [f"{c} is null" for c in raw_expr]
            null_exprs = " OR ".join(null_exprs)

            col_or_expr = f"""
                case when
                {null_exprs} then null
                else
                {col_or_expr}
                end
            """

        sql = f"""
        select * from
        (select
            count(*) as value_count,
            '{gn}' as group_name,
            cast({col_or_expr} as varchar) as value,
            (select count({col_or_expr}) from {table_name}) as total_non_null_rows,
            (select count(*) from {table_name}) as total_rows_inc_nulls,
            (select count(distinct {col_or_expr}) from {table_name})
                as distinct_value_count
        from {table_name}
        where {col_or_expr} is not null
        group by {col_or_expr}
        order by count(*) desc) column_stats
        """
        sqls.append(sql)

    return " union all ".join(sqls)


def _add_100_percentile_to_df_percentiles(percentile_rows):
    r = percentile_rows[0]
    if r["percentile_ex_nulls"] != 1.0:
        first_row = deepcopy(r)
        first_row["percentile_inc_nulls"] = 1.0
        first_row["percentile_ex_nulls"] = 1.0

    percentile_rows.append(first_row)
    return percentile_rows


def profile_columns(linker, column_expressions=None, top_n=10, bottom_n=10):
    """
    Profiles the specified columns of the dataframe initiated with the linker.

    This can be computationally expensive if the dataframe is large.

    For the provided columns with column_expressions (or for all columns if left empty)
    calculate:
    - A distribution plot that shows the count of values at each percentile.
    - A top n chart, that produces a chart showing the count of the top n values
    within the column
    - A bottom n chart, that produces a chart showing the count of the bottom
    n values within the column

    This should be used to explore the dataframe, determine if columns have
    sufficient completeness for linking, analyse the cardinality of columns, and
    identify the need for standardisation within a given column.

    Args:
        linker (object): The initiated linker.
        column_expressions (list, optional): A list of strings containing the
            specified column names.
            If left empty this will default to all columns.
        top_n (int, optional): The number of top n values to plot.
        bottom_n (int, optional): The number of bottom n values to plot.

    Returns:
        altair.Chart or dict: A visualization or JSON specification describing the
         profiling charts.

    Note:
        - The `linker` object should be an instance of the initiated linker.
        - The provided `column_expressions` can be a list of column names to profile.
            If left empty, all columns will be profiled.
        - The `top_n` and `bottom_n` parameters determine the number of top and bottom
            values to display in the respective charts.
    """

    if not column_expressions:
        column_expressions = [col.name for col in linker._input_columns()]

    df_concat = linker._initialise_df_concat()

    input_dataframes = []
    if df_concat:
        input_dataframes.append(df_concat)

    column_expressions_raw = ensure_is_list(column_expressions)
    column_expressions = expressions_to_sql(column_expressions_raw)

    sql = _col_or_expr_frequencies_raw_data_sql(
        column_expressions_raw, "__splink__df_concat"
    )

    linker._enqueue_sql(sql, "__splink__df_all_column_value_frequencies")
    df_raw = linker._execute_sql_pipeline(input_dataframes)

    sqls = _get_df_percentiles()
    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    df_percentiles = linker._execute_sql_pipeline([df_raw])
    percentile_rows_all = df_percentiles.as_record_dict()

    sql = _get_df_top_bottom_n(column_expressions, top_n, "desc")
    linker._enqueue_sql(sql, "__splink__df_top_n")
    df_top_n = linker._execute_sql_pipeline([df_raw])
    top_n_rows_all = df_top_n.as_record_dict()

    sql = _get_df_top_bottom_n(column_expressions, bottom_n, "asc")
    linker._enqueue_sql(sql, "__splink__df_bottom_n")
    df_bottom_n = linker._execute_sql_pipeline([df_raw])
    bottom_n_rows_all = df_bottom_n.as_record_dict()

    inner_charts = []

    for expression in column_expressions:
        percentile_rows = [
            p for p in percentile_rows_all if p["group_name"] == _group_name(expression)
        ]
        if percentile_rows == []:
            logger.warning(
                "Warning: No charts produced for "
                f"{expression}"
                " as the column only contains null values."
            )
        else:
            percentile_rows = _add_100_percentile_to_df_percentiles(percentile_rows)
            top_n_rows = [
                p for p in top_n_rows_all if p["group_name"] == _group_name(expression)
            ]
            bottom_n_rows = [
                p
                for p in bottom_n_rows_all
                if p["group_name"] == _group_name(expression)
            ]
            # remove concat blank from expression title
            expression = expression.replace(", ' '", "")
            inner_chart = _get_inner_chart_spec_freq(
                percentile_rows, top_n_rows, bottom_n_rows, expression
            )
            inner_charts.append(inner_chart)

    if inner_charts != []:
        outer_spec = deepcopy(_outer_chart_spec_freq)
        outer_spec["vconcat"] = inner_charts

        return altair_or_json(outer_spec)

    else:
        return None

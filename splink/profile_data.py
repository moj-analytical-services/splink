import logging
import re
from copy import deepcopy
from .charts import load_chart_definition, vegalite_or_json
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
    "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
}

_inner_chart_spec = load_chart_definition("profile_data.json")
_distribution_plotss_plot = load_chart_definition("profile_data_distribution_plots.json")
_top_n_plot = load_chart_definition("profile_data_top_n.json")
_bottom_n_plot = load_chart_definition("profile_data_bottom_n.json")
_kde_plot = load_chart_definition("profile_data_kde.json")

def _get_inner_chart_spec_freq(
        col_name,
        percentile_data=None, 
        top_n_data=None, 
        bottom_n_data=None, 
        kde_data=None,
        ):
    
    inner_spec = deepcopy(_inner_chart_spec)
    inner_specs=[]

    if percentile_data!=None:
        _distribution_plotss_plot_copy=deepcopy(_distribution_plotss_plot)
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
        _distribution_plotss_plot_copy["data"]["values"] = percentile_data
        _distribution_plotss_plot_copy["title"][
            "text"
        ] = f"Distribution of counts of values in column {col_name}"

        _distribution_plotss_plot_copy["title"]["subtitle"] = sub

        inner_specs.append(_distribution_plotss_plot_copy)

    if top_n_data!=None:
        _top_n_plot_copy=deepcopy(_top_n_plot)
        _top_n_plot_copy["data"]["values"] = top_n_data
        _top_n_plot_copy["title"] = f"Top {len(top_n_data)} values by value count"

        inner_specs.append(_top_n_plot_copy)

    if bottom_n_data!=None:
        _bottom_n_plot_copy=deepcopy(_bottom_n_plot)
        _bottom_n_plot_copy["data"]["values"] = bottom_n_data
        _bottom_n_plot_copy[
            "title"
        ] = f"Bottom {len(bottom_n_data)} values by value count"

        max_val = top_n_data[0]["value_count"]
        _bottom_n_plot_copy["encoding"]["y"]["scale"] = {"domain": [0, max_val]}

        inner_specs.append(_bottom_n_plot_copy)

    if kde_data!=None:
        _kde_plot_copy=deepcopy(_kde_plot)
        _kde_plot_copy["data"]["values"] = kde_data
        _kde_plot_copy["title"] = f"Kernel Density Estimation"
        _kde_plot_copy["mark"] = "area"
        _kde_plot_copy["encoding"]["x"]["field"] = "value"
        _kde_plot_copy["encoding"]["y"]["field"] = "value_count"

        inner_specs.append(_kde_plot_copy)

    inner_spec["hconcat"]=inner_specs

    return inner_spec


def _get_df_percentiles():
    """Take __splink__df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile chart
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

def _get_df_kde():
    sql = """
    select 
        value,
        value_count,
        group_name,
    from __splink__df_all_column_value_frequencies
    """
    return sql

def _get_df_top_bottom_n(expressions, limit=20, value_order="desc"):
    sql = """
    select * from
    (select *
    from __splink__df_all_column_value_frequencies
    where group_name = '{gn}'
    order by value_count {value_order}
    limit {limit})
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
        order by count(*) desc)
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


def profile_columns(
        linker, 
        column_expressions, 
        top_n=10, 
        bottom_n=10,
        distribution_plots=True,
        kde_plots=False,
        ):
    
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
    df_raw = linker._execute_sql_pipeline(input_dataframes, materialise_as_hash=True)

    #sqls = _get_df_kde()
    #for sql in sqls:
    #    linker.eqnqueue_sql(sql["sql"], sql["output_table_name"])
    if distribution_plots:
        sqls = _get_df_percentiles()
        for sql in sqls:
            linker._enqueue_sql(sql["sql"], sql["output_table_name"])
        df_percentiles = linker._execute_sql_pipeline([df_raw])
        percentile_rows_all = df_percentiles.as_record_dict()
    else: percentile_rows_all = None

    if top_n!=None:
        sql = _get_df_top_bottom_n(column_expressions, top_n, "desc")
        linker._enqueue_sql(sql, "__splink__df_top_n")
        df_top_n = linker._execute_sql_pipeline([df_raw])
        top_n_rows_all = df_top_n.as_record_dict()
    else: top_n_rows_all = None

    if kde_plots:
        sql = _get_df_kde()
        linker._enqueue_sql(sql, "__splink__df_kde")
        df_kde = linker._execute_sql_pipeline([df_raw])
        kde_rows_all = df_kde.as_record_dict()
    else: kde_rows_all = None

    if bottom_n!=None:
        sql = _get_df_top_bottom_n(column_expressions, bottom_n, "asc")
        linker._enqueue_sql(sql, "__splink__df_bottom_n")
        df_bottom_n = linker._execute_sql_pipeline([df_raw])
        bottom_n_rows_all = df_bottom_n.as_record_dict()
    else: bottom_n_rows_all = None

    inner_charts = []

    for expression in column_expressions:

        print(expression)
        print(_group_name(expression))
        if distribution_plots:
            percentile_rows = [
                p for p in percentile_rows_all if p["group_name"] == _group_name(expression)
            ]
            percentile_rows = _add_100_percentile_to_df_percentiles(percentile_rows)
        if top_n!=None:
            top_n_rows = [
                p for p in top_n_rows_all if p["group_name"] == _group_name(expression)
            ]
        if bottom_n!=None:
            bottom_n_rows = [
                p for p in bottom_n_rows_all if p["group_name"] == _group_name(expression)
            ]
        if kde_plots:
            kde_rows = [
                p for p in kde_rows_all if p["group_name"] == _group_name(expression)
            ]


        print("Creating inner chart")
        inner_chart = _get_inner_chart_spec_freq(
            percentile_data=percentile_rows,
            top_n_data=top_n_rows,
            bottom_n_data=bottom_n_rows,
            kde_data=kde_rows,
            col_name=expression,
        )


        print("Appending inner chart")

        inner_charts.append(inner_chart)

        print("Inner chart appended")

    outer_spec = deepcopy(_outer_chart_spec_freq)
    outer_spec["vconcat"] = inner_charts

    return vegalite_or_json(outer_spec)







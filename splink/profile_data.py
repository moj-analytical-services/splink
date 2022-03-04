import re


def _group_name(cols_or_exprs):
    group_name = "_".join(cols_or_exprs)
    group_name = re.sub(r"[^0-9a-zA-Z_\(\),]", " ", group_name)
    group_name = re.sub(r"\s+", " ", group_name)
    return group_name


def _get_df_percentiles():
    """Take df_all_column_value_frequencies and
    turn it into the raw data needed for the percentile cahrt
    """

    sqls = []

    sql = """
    select sum(value_count) as sum_tokens_in_value_count_group, value_count, group_name,
    first(total_non_null_rows) as total_non_null_rows,
    first(total_rows_inc_nulls) as total_rows_inc_nulls,
    first(distinct_value_count) as distinct_value_count
    from df_all_column_value_frequencies
    group by group_name, value_count
    order by group_name, value_count desc
    """

    sqls.append({"sql": sql, "output_table_name": "df_total_in_value_counts"})

    sql = """
    select sum(sum_tokens_in_value_count_group) over (partition by group_name order by value_count desc) as value_count_cumsum,
    sum_tokens_in_value_count_group, value_count, group_name,  total_non_null_rows, total_rows_inc_nulls, distinct_value_count
    from df_total_in_value_counts
    """
    sqls.append(
        {"sql": sql, "output_table_name": "df_total_in_value_counts_cumulative"}
    )

    sql = """
    select
    1 - (cast(value_count_cumsum as float)/total_non_null_rows) as percentile_ex_nulls,
    1 - (cast(value_count_cumsum as float)/total_rows_inc_nulls) as percentile_inc_nulls,

    value_count, group_name, total_non_null_rows, total_rows_inc_nulls,
    sum_tokens_in_value_count_group, distinct_value_count
    from df_total_in_value_counts_cumulative

    """
    sqls.append({"sql": sql, "output_table_name": "df_percentiles"})
    return sqls


def _col_or_expr_frequencies_raw_data_sql(col_or_expr, table_name):

    sql = f"""

    select
        count(*) as value_count,

        {col_or_expr} as value,
        "{col_or_expr}" as group_name,
        (select count({col_or_expr}) from {table_name}) as total_non_null_rows,
        (select count(*) from {table_name}) as total_rows_inc_nulls,
        (select count(distinct {col_or_expr}) from {table_name}) as distinct_value_count

    from {table_name}
    where {col_or_expr} is not null
    group by {col_or_expr}
    order by group_name, count(*) desc

    """

    return sql


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
    return linker.execute_sql_pipeline([df_raw])

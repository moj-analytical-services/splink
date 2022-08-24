def missingness_sqls(columns, input_tablename):

    sqls = []
    col_template = """
                select
                    count({col_name_escaped}) as non_null_count,
                    '{col_name}' as column_name
                from {input_tablename}"""

    selects = [
        col_template.format(
            col_name_escaped=col.name(),
            col_name=col.unquote().name(),
            input_tablename=input_tablename,
        )
        for col in columns
    ]

    sql = " union all ".join(selects)

    sqls.append(
        {
            "sql": sql,
            "output_table_name": "null_counts_for_columns",
        }
    )

    sql = f"""
    select
        1.0 - non_null_count/(select cast(count(*) as float)
        from {input_tablename}) as null_proportion,
        (select count(*) from {input_tablename}) - non_null_count as null_count,
        (select count(*) from {input_tablename}) as total_record_count,
        column_name
    from null_counts_for_columns
    """

    sqls.append({"sql": sql, "output_table_name": "missingness_data_for_chart"})

    return sqls


def missingness_data(linker, input_tablename):

    if input_tablename is None:
        input_tablename = "__splink__df_concat_with_tf"
        if not linker._table_exists_in_database("__splink__df_concat_with_tf"):
            linker._initialise_df_concat()
            input_tablename = "__splink__df_concat"

    splink_dataframe = linker._table_to_splink_dataframe(
        input_tablename, input_tablename
    )
    columns = splink_dataframe.columns

    sqls = missingness_sqls(columns, input_tablename)

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    df = linker._execute_sql_pipeline()

    return df.as_record_dict()


def completeness_data(linker, input_tablename=None, cols=None):
    sqls = []

    if input_tablename is None:
        input_tablename = "__splink__df_concat_with_tf"
        if not linker._table_exists_in_database("__splink__df_concat_with_tf"):
            linker._initialise_df_concat()
            input_tablename = "__splink__df_concat"

    columns = linker._settings_obj._columns_used_by_comparisons

    if linker._settings_obj._source_dataset_column_name_is_required:
        source_name = linker._settings_obj._source_dataset_column_name
    else:
        # Set source dataset to a literal string if dedupe_only
        source_name = "'_a'"

    for col in columns:
        sql = f"""
        (select
            {source_name} as source_dataset,
            '{col}' as column_name,
            count(*) - count({col}) as total_null_rows,
            count(*) as total_rows_inc_nulls,
            count({col})*1.0/count(*) as completeness
        from {input_tablename}
        group by source_dataset
        order by count(*) desc)
        """
        sqls.append(sql)

    sql = " union all ".join(sqls)

    df = linker._sql_to_splink_dataframe_checking_cache(
        sql, "__splink__df_all_column_completeness"
    )

    return df.as_record_dict()

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
            col_name=col.name(escape=False),
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

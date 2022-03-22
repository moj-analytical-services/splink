def row_examples(linker, example_rows_per_category=2):

    sqls = []

    uid_cols = linker.settings_obj._unique_id_columns
    uid_cols_l = [uid_col.name_l for uid_col in uid_cols]
    uid_cols_r = [uid_col.name_r for uid_col in uid_cols]
    uid_cols = uid_cols_l + uid_cols_r
    uid_expr = " || '-' ||".join(uid_cols)

    gamma_columns = [c.gamma_column_name for c in linker.settings_obj.comparisons]

    gam_concat = " || ',' || ".join(gamma_columns)

    sql = f"""
    select *, {uid_expr} as rec_comparison_id, {gam_concat} as gam_concat
    from __splink__df_predict
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_id",
    }
    sqls.append(sql)

    sql = """
    select *,
        ROW_NUMBER() OVER (PARTITION BY gam_concat) AS row_example_index
    from __splink__df_predict_with_row_id
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict_with_row_num",
    }
    sqls.append(sql)

    sql = f"""
    select *
    from __splink__df_predict_with_row_num
    where row_example_index <= {example_rows_per_category}
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_example_rows",
    }

    sqls.append(sql)

    return sqls


def comparison_viewer_table(linker, example_rows_per_category=2):

    sqls = row_examples(linker, example_rows_per_category)

    sql = """
    select ser.*,
           cvd.sum_gam,
           cvd.count_rows_in_comparison_vector_group,
           cvd.proportion_of_comparisons
    from __splink__df_example_rows as ser
    left join
     __splink__df_comparison_vector_distribution as cvd
    on ser.gam_concat = cvd.gam_concat
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_comparison_viewer_table",
    }

    sqls.append(sql)
    return sqls

    # Correlated subquey approach does not work in DuckDB
    # since the limit keyword doesn't seem to work as expected

    # sql = f"""
    # select *
    # from __splink__df_predict_with_row_id as p1
    # where  rec_comparison_id in
    #     (select rec_comparison_id
    #     from __splink__df_predict_with_row_id
    #     where gam_concat
    #           = p1.gam_concat

    #     limit 1)
    # """

    # sql = {
    #     "sql": sql,
    #     "output_table_name": "__splink__df_predict_examples_per_category",
    # }

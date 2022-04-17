import logging

logger = logging.getLogger(__name__)


def vertically_concatente_sql(linker):

    # Use column order from first table in dict
    df_obj = next(iter(linker.input_dfs.values()))
    columns = df_obj.columns_escaped

    select_columns_sql = ", ".join(columns)

    # For data profiling, we need to vertically concat
    # but user may not have provided a settings dict yet
    if linker._settings_obj is None:
        source_dataset_col_req = True
    else:
        source_dataset_col_req = (
            linker.settings_obj._source_dataset_column_name_is_required
        )

    if source_dataset_col_req:
        sqls_to_union = []
        for df_obj in linker.input_dfs.values():
            sql = f"""
            select '{df_obj.templated_name}' as source_dataset, {select_columns_sql}
            from {df_obj.physical_name}
            """
            sqls_to_union.append(sql)
        sql = " UNION ALL ".join(sqls_to_union)
    else:
        sql = f"""
            select {select_columns_sql}
            from {df_obj.physical_name}
            """

    return sql

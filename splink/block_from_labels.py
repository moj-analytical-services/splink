from .lower_id_on_lhs import lower_id_to_left_hand_side


def block_from_labels(linker, table_name):

    df = linker._df_as_obj(table_name, table_name)

    unique_id_col = linker.settings_obj._unique_id_column_name

    source_dataset_col = linker.settings_obj._source_dataset_column_name

    sql = lower_id_to_left_hand_side(df, source_dataset_col, unique_id_col)

    sqls = []
    sql = {
        "sql": sql,
        "output_table_name": "__splink__labels_prepared_for_joining",
    }

    sqls.append(sql)

    columns_to_select = linker.settings_obj._columns_to_select_for_blocking
    sql_select_expr = ", ".join(columns_to_select)

    if linker.settings_obj._source_dataset_column_name_is_required:
        join_condition_l = f"""
        l.{source_dataset_col} = df_labels.{source_dataset_col}_l and
        l.{unique_id_col} = df_labels.{unique_id_col}_l
        """
        join_condition_r = f"""
        r.{source_dataset_col} = df_labels.{source_dataset_col}_r and
        r.{unique_id_col} = df_labels.{unique_id_col}_r
        """
    else:
        join_condition_l = f"l.{unique_id_col} = df_labels.{unique_id_col}_l"
        join_condition_r = f"r.{unique_id_col} = df_labels.{unique_id_col}_r"

    sql = f"""
    select {sql_select_expr}, 'from_labels' as match_key from
    __splink__labels_prepared_for_joining as df_labels
    inner join __splink__df_concat_with_tf as l
    on {join_condition_l}
    inner join __splink__df_concat_with_tf as r
    on {join_condition_r}
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_blocked",
    }

    sqls.append(sql)

    return sqls

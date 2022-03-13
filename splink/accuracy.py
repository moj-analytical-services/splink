from .block_from_labels import block_from_labels
from .comparison_vector_values import compute_comparison_vector_values
from .predict import predict
from .sql_transform import move_l_r_table_prefix_to_column_suffix


def predict_scores_for_labels(linker, labels_tablename):

    sqls = block_from_labels(linker, labels_tablename)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = compute_comparison_vector_values(linker.settings_obj)

    linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sqls = predict(linker.settings_obj)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sds_col = linker.settings_obj._source_dataset_column_name
    uid_col = linker.settings_obj._unique_id_column_name

    brs = linker.settings_obj._blocking_rules_to_generate_predictions
    if brs:
        brs = [move_l_r_table_prefix_to_column_suffix(b) for b in brs]
        brs = [f"(coalesce({b}, false))" for b in brs]
        brs = " OR ".join(brs)
        br_col = f"({brs})"
    else:
        br_col = " 1=1 "

    if len(linker.settings_obj._unique_id_columns) == 1:
        join_conditions = f"""
            pred.{uid_col}_l = lab.{uid_col}_l and
            pred.{uid_col}_r = lab.{uid_col}_r
        """
    else:
        join_conditions = f"""
            pred.{sds_col}_l = lab.{sds_col}_l and
            pred.{sds_col}_r = lab.{sds_col}_r and
            pred.{uid_col}_l = lab.{uid_col}_l and
            pred.{uid_col}_r = lab.{uid_col}_r
        """

    sql = f"""
    select lab.clerical_match_score,
    {br_col} as found_by_blocking_rules,
     pred.*
    from __splink__df_predict as pred
    left join __splink__labels_prepared_for_joining as lab
    on {join_conditions}

    """
    linker.enqueue_sql(sql, "__splink__labels_with_predictions")

    df_predict = linker.execute_sql_pipeline()
    return df_predict

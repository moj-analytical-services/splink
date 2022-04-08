from .block_from_labels import block_from_labels
from .comparison_vector_values import compute_comparison_vector_values_sql
from .predict import predict_from_comparison_vectors_sql
from .sql_transform import move_l_r_table_prefix_to_column_suffix


def predict_scores_for_labels(linker, labels_tablename):

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

    if linker.settings_obj._source_dataset_column_name_is_required:
        join_conditions = f"""
            pred.{sds_col}_l = lab.{sds_col}_l and
            pred.{sds_col}_r = lab.{sds_col}_r and
            pred.{uid_col}_l = lab.{uid_col}_l and
            pred.{uid_col}_r = lab.{uid_col}_r
        """
    else:
        join_conditions = f"""
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

    return sql


def truth_space_table_from_labels_with_predictions(threshold_actual=0.5):

    # c_P and c_N are clerical positive and negative, respectively
    sqls = []
    sql = f"""
    select
    *,
    match_weight as truth_threshold,
    case when clerical_match_score >= {threshold_actual} then 1
    else 0
    end
    as c_P,
    case when clerical_match_score >= {threshold_actual} then 0
    else 1
    end
    as c_N
    from __splink__labels_with_predictions
    order by match_weight
    """

    sql = {"sql": sql, "output_table_name": "__splink__labels_with_pos_neg"}
    sqls.append(sql)

    sql = """
    select
        truth_threshold,
        count(*) as num_records_in_row,
        sum(c_P) as c_P,
        sum(c_N) as c_N
    from
    __splink__labels_with_pos_neg
    group by truth_threshold
    order by truth_threshold
    """

    sql = {"sql": sql, "output_table_name": "__splink__labels_with_pos_neg_grouped"}
    sqls.append(sql)

    sql = """
    select
    truth_threshold,

    (sum(c_P) over (order by truth_threshold desc))  as cum_clerical_P,
    (sum(c_N) over (order by truth_threshold)) - c_N as cum_clerical_N,

    (select sum(c_P) from __splink__labels_with_pos_neg_grouped) as total_clerical_P,
    (select sum(c_N) from __splink__labels_with_pos_neg_grouped) as total_clerical_N,

    (select sum(num_records_in_row) from __splink__labels_with_pos_neg_grouped)
        as row_count,

    -num_records_in_row + sum(num_records_in_row) over (order by truth_threshold)
        as N_labels,

    sum(num_records_in_row) over (order by truth_threshold desc) as P_labels
    from __splink__labels_with_pos_neg_grouped
    order by  truth_threshold
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_stats",
    }
    sqls.append(sql)

    sql = """
    select
    truth_threshold,
    row_count,
    total_clerical_P as P,
    total_clerical_N as N,

    P_labels - cum_clerical_P as FP,
    cum_clerical_P as TP,

    N_labels - cum_clerical_N as FN,
    cum_clerical_N as TN

    from __splink__labels_with_pos_neg_grouped_with_stats
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_pos_neg_grouped_with_truth_stats",
    }
    sqls.append(sql)

    sql = """
    select
    truth_threshold,
    row_count,
    P,
    N,
    TP,
    TN,
    FP,
    FN,
    P/row_count as P_rate,
    cast(N as float)/row_count as N_rate,
    cast(TP as float)/P as TP_rate,
    cast(TN as float)/N as TN_rate,
    cast(FP as float)/N as FP_rate,
    cast(FN as float)/P as FN_rate,
    cast(TP as float)/(TP+FP) as precision,
    cast(TP as float)/(TP+FN) as recall
    from __splink__labels_with_pos_neg_grouped_with_truth_stats
    """

    sql = {"sql": sql, "output_table_name": "__splink__truth_space_table"}
    sqls.append(sql)
    return sqls


def truth_space_table(linker, labels_tablename, threshold_actual=0.5):
    sqls = block_from_labels(linker, labels_tablename)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = compute_comparison_vector_values_sql(linker.settings_obj)

    linker.enqueue_sql(sql, "__splink__df_comparison_vectors")

    sqls = predict_from_comparison_vectors_sql(linker.settings_obj)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    sql = predict_scores_for_labels(linker, labels_tablename)
    linker.enqueue_sql(sql, "__splink__labels_with_predictions")

    # c_P and c_N are clerical positive and negative, respectively
    sqls = truth_space_table_from_labels_with_predictions(threshold_actual)

    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])

    df_truth_space_table = linker.execute_sql_pipeline()

    return df_truth_space_table

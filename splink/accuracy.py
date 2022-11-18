from copy import deepcopy


from .block_from_labels import block_from_labels
from .comparison_vector_values import compute_comparison_vector_values_sql
from .predict import predict_from_comparison_vectors_sqls
from .blocking import BlockingRule
from .sql_transform import move_l_r_table_prefix_to_column_suffix


def truth_space_table_from_labels_with_predictions_sqls(
    threshold_actual=0.5, match_weight_round_to_nearest=None
):

    # Round to match_weight_round_to_nearest.
    # e.g. if it's 0.25, 1.27 gets rounded to 1.25
    if match_weight_round_to_nearest is not None:
        truth_thres_expr = f"""
            cast({match_weight_round_to_nearest} as float) *
            (round(match_weight/{match_weight_round_to_nearest}))
        """
    else:
        truth_thres_expr = "match_weight"

    # c_P and c_N are clerical positive and negative, respectively
    sqls = []
    sql = f"""
    select
    *,
    {truth_thres_expr} as truth_threshold,
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
        power(2, truth_threshold) / (1 + power(2, truth_threshold))
            as match_probability,
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
        cast(TP as float)/(TP+FN) as recall,
        cast(TP as float)/(TP + (FP + FN)/2) as F1
    from __splink__labels_with_pos_neg_grouped_with_truth_stats
    """

    sql = {"sql": sql, "output_table_name": "__splink__truth_space_table"}
    sqls.append(sql)
    return sqls


def _select_found_by_blocking_rules(linker):
    brs = linker._settings_obj._blocking_rules_to_generate_predictions
    if brs:
        brs = [move_l_r_table_prefix_to_column_suffix(b.blocking_rule) for b in brs]
        brs = [f"(coalesce({b}, false))" for b in brs]
        brs = " OR ".join(brs)
        br_col = f" ({brs}) "
    else:
        br_col = " 1=1 "

    return f"{br_col} as found_by_blocking_rules"


def truth_space_table_from_labels_table(
    linker, labels_tablename, threshold_actual=0.5, match_weight_round_to_nearest=None
):

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    # c_P and c_N are clerical positive and negative, respectively
    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual, match_weight_round_to_nearest
    )

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    df_truth_space_table = linker._execute_sql_pipeline()

    return df_truth_space_table


def truth_space_table_from_labels_column(
    linker, label_colname, threshold_actual=0.5, match_weight_round_to_nearest=None
):

    new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)

    df_predict = _predict_from_label_column_sql(
        linker,
        label_colname,
    )

    sql = f"""
    select
    cast(({label_colname}_l = {label_colname}_r) as float) as clerical_match_score,
    not (cast(match_key as int) = {new_matchkey})
        as found_by_blocking_rules,
    *
    from {df_predict.physical_name}
    """

    linker._enqueue_sql(sql, "__splink__labels_with_predictions")

    # c_P and c_N are clerical positive and negative, respectively
    sqls = truth_space_table_from_labels_with_predictions_sqls(
        threshold_actual, match_weight_round_to_nearest
    )

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    df_truth_space_table = linker._execute_sql_pipeline()

    return df_truth_space_table


def predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename):
    sqls = block_from_labels(
        linker, labels_tablename, include_clerical_match_score=True
    )

    sql = {
        "sql": compute_comparison_vector_values_sql(
            linker._settings_obj, include_clerical_match_score=True
        ),
        "output_table_name": "__splink__df_comparison_vectors",
    }

    sqls.append(sql)

    sqls_2 = predict_from_comparison_vectors_sqls(
        linker._settings_obj,
        include_clerical_match_score=True,
        sql_infinity_expression=linker._infinity_expression,
    )

    sqls.extend(sqls_2)
    br_col = _select_found_by_blocking_rules(linker)

    sql = f"""
    select *, {br_col}
    from __splink__df_predict
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__labels_with_predictions",
    }

    # Clearer name than just df_predict
    sqls.append(sql)

    return sqls


def prediction_errors_from_labels_table(
    linker,
    labels_tablename,
    include_false_positives=True,
    include_false_negatives=True,
    threshold=0.5,
):
    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, labels_tablename)

    for sql in sqls:
        linker._enqueue_sql(sql["sql"], sql["output_table_name"])

    false_positives = f"""
    (clerical_match_score < {threshold} and
    match_probability > {threshold})
    """

    false_negatives = f"""
    (clerical_match_score > {threshold} and
    match_probability < {threshold})
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select *,
    case
    when {false_positives} then 'FP'
    when {false_negatives} then 'FN'
    END as truth_status

    from __splink__labels_with_predictions
    where
    {where_condition}
    """

    linker._enqueue_sql(sql, "__splink__labels_with_fp_fn_status")

    return linker._execute_sql_pipeline()


def _predict_from_label_column_sql(linker, label_colname):

    # In the case of labels, we use them to block
    # In the case we have a label column, we want to apply the model's blocking rules
    # but add in blocking on the label colname
    linker = deepcopy(linker)
    settings = linker._settings_obj
    brs = settings._blocking_rules_to_generate_predictions

    label_blocking_rule = BlockingRule(f"l.{label_colname} = r.{label_colname}")
    label_blocking_rule.preceding_rules = brs.copy()
    brs.append(label_blocking_rule)

    # Need the label colname to be in additional columns to retain

    add_cols = settings._additional_columns_to_retain_list

    if label_colname not in add_cols:
        settings._additional_columns_to_retain_list.append(label_colname)

    # Now we want to create predictions
    df_predict = linker.predict()

    return df_predict


def prediction_errors_from_label_column(
    linker,
    label_colname,
    include_false_positives=True,
    include_false_negatives=True,
    threshold=0.5,
):

    df_predict = _predict_from_label_column_sql(
        linker,
        label_colname,
    )

    # Clerical match score is 1 where the label_colname is equal else zero

    # _predict_from_label_column_sql will add a match key for matching on labels
    new_matchkey = len(linker._settings_obj._blocking_rules_to_generate_predictions)

    sql = f"""
    select
    cast(({label_colname}_l = {label_colname}_r) as float) as clerical_match_score,
    not (cast(match_key as int) = {new_matchkey})
        as found_by_blocking_rules,
    *
    from {df_predict.physical_name}
    """

    # found_by_blocking_rules

    linker._enqueue_sql(sql, "__splink__predictions_from_label_column")

    false_positives = f"""
    (clerical_match_score < {threshold} and
    match_probability > {threshold})
    """

    false_negatives = f"""
    ((clerical_match_score > {threshold} and
    match_probability < {threshold})
    or
    (clerical_match_score > {threshold} and
     found_by_blocking_rules = False)
    )
    """

    where_conditions = []
    if include_false_positives:
        where_conditions.append(false_positives)

    if include_false_negatives:
        where_conditions.append(false_negatives)

    where_condition = " OR ".join(where_conditions)

    sql = f"""
    select * from __splink__predictions_from_label_column
    where {where_condition}
    """

    linker._enqueue_sql(sql, "__splink__predictions_from_label_column_fp_fn_only")

    predictions = linker._execute_sql_pipeline()

    return predictions

from .settings import complete_settings_dict

from functools import reduce
from pyspark.sql import DataFrame


def _sql_gen_unique_id_keygen(table, uid_col1, uid_col2):

    return f"""
    case
    when {table}.{uid_col1} > {table}.{uid_col2} then concat({table}.{uid_col2}, '-', {table}.{uid_col1})
    else concat({table}.{uid_col1}, '-', {table}.{uid_col2})
    end
    """


def _get_score_colname(settings):
    score_colname = "match_probability"
    for c in settings["comparison_columns"]:
        if c["term_frequency_adjustments"]:
            score_colname = "tf_adjusted_match_prob"
    return score_colname


def _join_labels_to_results(df_labels, df_e, settings, spark):

    # df_labels is a dataframe like:
    # | unique_id_l | unique_id_r | clerical_match_score |
    # |:------------|:------------|---------------------:|
    # | id1         | id2         |                  0.9 |
    # | id1         | id3         |                  0.1 |
    settings = complete_settings_dict(settings, None)
    uid_colname = settings["unique_id_column_name"]

    # If settings has tf_afjustments, use tf_adjusted_match_prob else use match_probability
    score_colname = _get_score_colname(settings)

    # The join is trickier than it looks because there's no guarantee of which way around the two ids are
    # it could be id1, id2 in df_labels and id2,id1 in df_e

    uid_col_l = f"{uid_colname}_l"
    uid_col_r = f"{uid_colname}_r"

    df_labels.createOrReplaceTempView("df_labels")
    df_e.createOrReplaceTempView("df_e")

    sql = f"""
    select

    df_labels.{uid_col_l},
    df_labels.{uid_col_r},
    clerical_match_score,

    case
    when {score_colname} is null then 0
    else {score_colname}
    end as {score_colname},

    case
    when {score_colname} is null then false
    else true
    end as found_by_blocking


    from df_labels
    left join df_e
    on {_sql_gen_unique_id_keygen('df_labels', uid_col_l, uid_col_r)}
    = {_sql_gen_unique_id_keygen('df_e', uid_col_l, uid_col_r)}

    """

    return spark.sql(sql)


def _categorise_scores_into_truth_cats(
    df_e_with_labels, threshold_pred, spark, threshold_actual=0.5
):

    df_e_with_labels.createOrReplaceTempView("df_e_with_labels")

    pred = f"(tf_adjusted_match_prob > {threshold_pred})"

    actual = f"(clerical_match_score > {threshold_actual})"

    sql = f"""
    select
    *,
    {threshold_pred} as truth_threshold,
    {actual} = 1.0 as P,
    {actual} = 0.0 as N,
    {pred} = 1.0 and {actual} = 1.0 as TP,
    {pred} = 0.0 and {actual} = 0.0 as TN,
    {pred} = 1.0 and {actual} = 0.0 as FP,
    {pred} = 0.0 and {actual} = 1.0 as FN

    from
    df_e_with_labels

    """

    return spark.sql(sql)


def _summarise_truth_cats(df_truth_cats, spark):

    df_truth_cats.createOrReplaceTempView("df_truth_cats")

    sql = """

    select
    avg(truth_threshold) as truth_threshold,
    count(*) as row_count,
    sum(cast(P as int)) as P,
    sum(cast(N as int)) as N,
    sum(cast(TP as int)) as TP,
    sum(cast(TN as int)) as TN,
    sum(cast(FP as int)) as FP,
    sum(cast(FN as int)) as FN

    from df_truth_cats
    """

    df_truth_cats = spark.sql(sql)

    df_truth_cats.createOrReplaceTempView("df_truth_cats")

    sql = f"""

    select
    *,
    P/row_count as P_rate,
    N/row_count as N_rate,
    TP/row_count as TP_rate,
    TN/row_count as TN_rate,
    FP/row_count as FP_rate,
    FN/row_count as FN_rate

    from df_truth_cats
    """

    return spark.sql(sql)


def df_e_with_truth_categories(
    df_labels, df_e, settings, threshold_pred, spark, threshold_actual=0.5
):
    df_labels = _join_labels_to_results(df_labels, df_e, settings, spark)
    df_e_t = _categorise_scores_into_truth_cats(
        df_labels, threshold_pred, spark, threshold_actual
    )
    return df_e_t


def roc_table(df_labels, df_e, settings, spark, threshold_actual=0.5):
    df_labels_results = _join_labels_to_results(df_labels, df_e, settings, spark)

    # This is used repeatedly to generate the roc curve
    df_labels_results.persist()

    # We want percentiles of score to compute
    score_colname = _get_score_colname(settings)

    percentiles = [x / 100 for x in range(0, 101)]
    thresholds = df_labels_results.stat.approxQuantile(score_colname, percentiles, 0.0)
    thresholds = sorted(set(thresholds))

    roc_dfs = []
    for thres in thresholds:
        df_e_t = _categorise_scores_into_truth_cats(
            df_labels_results, thres, spark, threshold_actual
        )
        df_roc_row = _summarise_truth_cats(df_e_t, spark)
        roc_dfs.append(df_roc_row)

    all_roc_df = reduce(DataFrame.unionAll, roc_dfs)
    return all_roc_df

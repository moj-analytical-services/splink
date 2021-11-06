from functools import reduce
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
import pyspark
from typing import Union

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


def _sql_gen_unique_id_keygen(
    table: str,
    uid_col1: str,
    uid_col2: str,
    source_dataset1: str = None,
    source_dataset2: str = None,
):
    """Create a composite unique id for a pairwise comparisons
    This is a concatenation of the unique id of each record
    of the pairwise comparisons.

    The composite unique id is agnostic to the ordering
    i.e. it treats:
    unique_id_l = x, unique_id_r = y
    and
    unique_id_l = y, unique_id_r = x
    to be equivalent.

    This is necessarily because we cannot predict
    which way round they will appear

    Args:
        table (str): name of table
        uid_col1 (str): name of unique id col 1
        uid_col2 (str): name of unique id col 2
        source_dataset1 (str, optional): Name of source dataset column if exists. Defaults to None.
        source_dataset2 (str, optional): [description]. Defaults to None.

    Returns:
        str: sql case expression that outputs the composite unique_id
    """

    if source_dataset1:
        concat_1 = f"concat({table}.{source_dataset1}, '_',{table}.{uid_col1})"
        concat_2 = f"concat({table}.{source_dataset2}, '_',{table}.{uid_col2})"
    else:
        concat_1 = f"{table}.{uid_col1}"
        concat_2 = f"{table}.{uid_col2}"

    return f"""
    case
    when {concat_1} > {concat_2}
        then concat({concat_2}, '-', {concat_1})
    else concat({concat_1}, '-', {concat_2})
    end
    """


def _get_score_colname(df_e, score_colname=None):
    if score_colname:
        return score_colname
    elif "match_probability" in df_e.columns:
        return "match_probability"
    else:
        raise ValueError("There doesn't appear to be a score column in df_e")


def dedupe_splink_scores(
    df_e_with_dupes: DataFrame,
    unique_id_colname: str,
    score_colname: str = None,
    selection_fn: str = "abs_val",
):
    """Sometimes, multiple Splink jobs with different blocking rules are combined
    into a single dataset of edges.  Sometimes,the same pair of nodes will be
    scored multiple times, once by each job.  We need to deduplicate this dataset
    so each pair of nodes appears only once

    Args:
        df_e_with_dupes (DataFrame): Dataframe with dupes
        unique_id_colname (str): Unique id column name e.g. unique_id
        score_colname (str, optional): Which column contains scores? If none, inferred from
            df_e_with_dupes.columns. Defaults to None.
        selection_fn (str, optional): Where we have several different scores for a given
            pair of records, how do we decide the final score?
            Options are 'abs_val' and 'mean'.
            abs_val:  Take the value furthest from 0.5 i.e. the value that expresses most certainty
            mean: Take the mean of all values
            Defaults to 'abs_val'.
    """

    # Looking in blocking.py, the position of unique ids
    # (whether they appear in _l or _r) is guaranteed
    # in blocking outputs so we don't need to worry about
    # inversions

    # This is not the case for labelled data - hence the need
    # _sql_gen_unique_id_keygen to join labels to df_e

    possible_vals = ["abs_val", "mean"]
    if selection_fn not in possible_vals:
        raise ValueError(
            f"selection function should be in {possible_vals}, you passed {selection_fn}"
        )

    score_colname = _get_score_colname(df_e_with_dupes, score_colname)

    if selection_fn == "abs_val":
        df_e_with_dupes = df_e_with_dupes.withColumn(
            "absval", f.expr(f"0.5 - abs({score_colname})")
        )

        win_spec = Window.partitionBy(
            [f"{unique_id_colname}_l", f"{unique_id_colname}_r"]
        ).orderBy(f.col("absval").desc())
        df_e_with_dupes = df_e_with_dupes.withColumn(
            "ranking", f.row_number().over(win_spec)
        )
        df_e = df_e_with_dupes.filter(f.col("ranking") == 1)
        df_e = df_e.drop("absval")
        df_e = df_e.drop("ranking")

    if selection_fn == "mean":

        win_spec = Window.partitionBy(
            [f"{unique_id_colname}_l", f"{unique_id_colname}_r"]
        ).orderBy(f.col(score_colname).desc())

        df_e_with_dupes = df_e_with_dupes.withColumn(
            "ranking", f.row_number().over(win_spec)
        )

        df_e_with_dupes = df_e_with_dupes.withColumn(
            score_colname,
            f.avg(score_colname).over(
                win_spec.rowsBetween(
                    Window.unboundedPreceding, Window.unboundedFollowing
                )
            ),
        )
        df_e = df_e_with_dupes.filter(f.col("ranking") == 1)

        df_e = df_e.drop("ranking")

    return df_e


def labels_with_splink_scores(
    df_labels,
    df_e,
    unique_id_colname,
    spark,
    score_colname=None,
    source_dataset_colname=None,
    retain_all_cols=False,
):
    """Create a dataframe with clerical labels set against splink scores

    Assumes uniqueness of pairs of identifiers in both datasets - e.g.
    if you have duplicate clerical labels or splink scores, you should
    deduplicate them first

    Args:
        df_labels:  a dataframe like:
             | unique_id_l | unique_id_r | clerical_match_score |
             |:------------|:------------|---------------------:|
             | id1         | id2         |                  0.9 |
             | id1         | id3         |                  0.1 |
        df_e: a dataframe like
             | unique_id_l| unique_id_r| match_probability      |
             |:-----------|:-----------|-----------------------:|
             | id1        | id2        |                   0.85 |
             | id1        | id3        |                   0.2  |
             | id2        | id3        |                   0.1  |
        unique_id_colname (str): Unique id column name e.g. unique_id
        spark : SparkSession
        score_colname (float, optional): Allows user to explicitly state the column name
            in the Splink dataset containing the Splink score.  If none will be inferred
        source_dataset_colname (str, optional): if your datasets contain a source_dataset column, what name is it?
            and set join_on_source_dataset=True, which will include the source dataset in the join key Defaults to False.
        retain_all_cols (bool, optional): Retain all columns in input datasets. Defaults to False.

    Returns:
        DataFrame: Like:
             |   unique_id_l |   unique_id_r |   clerical_match_score |   match_probability      | found_by_blocking   |
             |--------------:|--------------:|-----------------------:|-------------------------:|:--------------------|
             |             0 |             1 |                      1 |                 0.999566 | True                |
             |             0 |             2 |                      1 |                 0.999566 | True                |
             |             0 |             3 |                      1 |                 0.999989 | True                |

    """
    score_colname = _get_score_colname(df_e, score_colname)

    uid_col_l = f"{unique_id_colname}_l"
    uid_col_r = f"{unique_id_colname}_r"

    df_labels.createOrReplaceTempView("df_labels")
    df_e.createOrReplaceTempView("df_e")

    if source_dataset_colname:
        source_ds_col_l = f"{source_dataset_colname}_l"
        source_ds_col_r = f"{source_dataset_colname}_r"
        labels_key = _sql_gen_unique_id_keygen(
            "df_labels", uid_col_l, uid_col_r, source_ds_col_l, source_ds_col_r
        )
        df_e_key = _sql_gen_unique_id_keygen(
            "df_e", uid_col_l, uid_col_r, source_ds_col_l, source_ds_col_r
        )
        select_cols = f"""
          coalesce(df_e.{source_ds_col_l},df_labels.{source_ds_col_l}) as {source_ds_col_l},
          coalesce(df_e.{uid_col_l},df_labels.{uid_col_l}) as {uid_col_l},
          coalesce(df_e.{source_ds_col_r},df_labels.{source_ds_col_r}) as {source_ds_col_r},
          coalesce(df_e.{uid_col_r},df_labels.{uid_col_r}) as {uid_col_r}
        """
    else:
        labels_key = _sql_gen_unique_id_keygen("df_labels", uid_col_l, uid_col_r)
        df_e_key = _sql_gen_unique_id_keygen("df_e", uid_col_l, uid_col_r)
        select_cols = f"""
          coalesce(df_e.{uid_col_l},df_labels.{uid_col_l}) as {uid_col_l},
          coalesce(df_e.{uid_col_r},df_labels.{uid_col_r}) as {uid_col_r}
        """

    if retain_all_cols:
        cols1 = [f"df_e.{c} as df_e__{c}" for c in df_e.columns if c != score_colname]
        cols2 = [
            f"df_labels.{c} as df_labels__{c}"
            for c in df_labels.columns
            if c != "clerical_match_score"
        ]
        cols1.extend(cols2)

        select_smt = ", ".join(cols1)

        sql = f"""
        select

        {select_smt},
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
        on {labels_key}
        = {df_e_key}

        """

    else:
        sql = f"""
        select

        {select_cols},
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
        on {labels_key}
        = {df_e_key}

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
    TP/P as TP_rate,
    TN/N as TN_rate,
    FP/N as FP_rate,
    FN/P as FN_rate,
    TP/(TP+FP) as precision,
    TP/(TP+FN) as recall

    from df_truth_cats
    """

    return spark.sql(sql)


def df_e_with_truth_categories(
    df_labels_with_splink_scores,
    threshold_pred,
    spark: SparkSession,
    threshold_actual: float = 0.5,
    score_colname: str = None,
):
    """Join Splink's predictions to clerically labelled data and categorise
    rows by truth category (false positive, true positive etc.)

    Note that df_labels

    Args:
        df_labels_with_splink_scores (DataFrame): A dataframe of labels and associated splink scores
            usually the output of the truth.labels_with_splink_scores function
        threshold_pred (float): Threshold to use in categorising Splink predictions into
            match or no match
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        score_colname (float, optional): Allows user to explicitly state the column name
            in the Splink dataset containing the Splink score.  If none will be inferred

    Returns:
        DataFrame: Dataframe of labels associated with truth category
    """

    df_labels_with_splink_scores.createOrReplaceTempView("df_labels_with_splink_scores")

    score_colname = _get_score_colname(df_labels_with_splink_scores)

    pred = f"({score_colname} >= {threshold_pred})"

    actual = f"(clerical_match_score >= {threshold_actual})"

    sql = f"""
    select
    *,
    cast ({threshold_pred} as float) as truth_threshold,
    {actual} = 1.0 as P,
    {actual} = 0.0 as N,
    {pred} = 1.0 and {actual} = 1.0 as TP,
    {pred} = 0.0 and {actual} = 0.0 as TN,
    {pred} = 1.0 and {actual} = 0.0 as FP,
    {pred} = 0.0 and {actual} = 1.0 as FN

    from
    df_labels_with_splink_scores

    """

    return spark.sql(sql)


def _truth_space_table_old(
    df_labels_with_splink_scores: DataFrame,
    spark: SparkSession,
    threshold_actual: float = 0.5,
    score_colname: str = None,
):
    """Create a table of the ROC space i.e. truth table statistics
    for each discrimination threshold

    Args:
        df_labels_with_splink_scores (DataFrame): A dataframe of labels and associated splink scores
            usually the output of the truth.labels_with_splink_scores function
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        score_colname (float, optional): Allows user to explicitly state the column name
            in the Splink dataset containing the Splink score.  If none will be inferred

    Returns:
        DataFrame: Table of 'truth space' i.e. truth categories for each threshold level
    """

    # This is used repeatedly to generate the roc curve
    df_labels_with_splink_scores.persist()

    # We want percentiles of score to compute
    score_colname = _get_score_colname(df_labels_with_splink_scores, score_colname)

    percentiles = [x / 100 for x in range(0, 101)]

    values_distinct = df_labels_with_splink_scores.select(score_colname).distinct()
    thresholds = values_distinct.stat.approxQuantile(score_colname, percentiles, 0.0)
    thresholds.append(1.01)
    thresholds = sorted(set(thresholds))

    roc_dfs = []
    for thres in thresholds:
        df_e_t = df_e_with_truth_categories(
            df_labels_with_splink_scores, thres, spark, threshold_actual, score_colname
        )
        df_roc_row = _summarise_truth_cats(df_e_t, spark)
        roc_dfs.append(df_roc_row)

    all_roc_df = reduce(DataFrame.unionAll, roc_dfs)
    return all_roc_df


def truth_space_table(
    df_labels_with_splink_scores: DataFrame,
    spark: SparkSession,
    threshold_actual: float = 0.5,
    score_colname: str = None,
):
    """Create a table of the ROC space i.e. truth table statistics
    for each discrimination threshold

    Args:
        df_labels_with_splink_scores (DataFrame): A dataframe of labels and associated splink scores
            usually the output of the truth.labels_with_splink_scores function
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        score_colname (float, optional): Allows user to explicitly state the column name
            in the Splink dataset containing the Splink score.  If none will be inferred

    Returns:
        DataFrame: Table of 'truth space' i.e. truth categories for each threshold level
    """

    # At a truth threshold of 1.0, we say a splink score of 1.0 is a positive in ROC space. i.e it's inclusive, so if there are splink scores of exactly 1.0 it's not possible to have zero positives in the truth table.
    # This means that at a truth threshold of 0.0 we say a splink score of 0.0 positive.  so it's possible to have zero negatives in the truth table.

    # This code provides an efficient way to compute the truth space
    # It's more complex than the previous code, but executes much faster because only a single SQL query/PySpark Action is needed
    # The previous implementation, which is easier to understand, is [here](https://github.com/moj-analytical-services/splink/blob/b4f601e6d180c6abfd64ab40775dca3e3513c0b5/splink/truth.py#L396)

    # We start with df_labels_with_splink_scores
    # This is a table of each pair of clerically labelled records accompanied by the Splink match score.
    # It is sorted in order to clerical_match_score, low to high

    # This means for any row, if we picked a threshold equal to clerical_match_score, all rows _above_ (in the table) are categoried by splink as non-matches.

    # For instance, if a clerical_match_score is 0.25, then any records above this in the table have a score of <0.25, and are therefore negative.  We categorise a score of exactly 0.25 as positive.

    # In addition, we can categorise any indiviual row as containing a false positive or false negative _at_ the clerical match score for the row.

    # This allows us to say things like:  Of the records above this row, we have _classified_ them all as negative, but we have _seen_ to true (clerically labelled) positives.  Thefore these must be false negatives.

    # In particular, the calculations are as follows:
    # False positives:  The cumulative total of positive labels in records BELOW this row, INCLUSIVE (because this one is being counted as positive)
    # True positives:  The total number of positives minus false positives

    # False negatives:  The total number of negatives, minus negatives seen above this row
    # True negatives:  The cumulative total of negative labels in records aboev this row

    # We want percentiles of score to compute
    score_colname = _get_score_colname(df_labels_with_splink_scores, score_colname)

    df_labels_with_splink_scores.createOrReplaceTempView("df_labels_with_splink_scores")
    sql = f"""
    select
    *,
    {score_colname} as truth_threshold,
    case when clerical_match_score >= {threshold_actual} then 1
    else 0
    end
    as c_P,
    case when clerical_match_score >= {threshold_actual} then 0
    else 1
    end
    as c_N
    from df_labels_with_splink_scores
    order by {score_colname}
    """
    df_with_labels = spark.sql(sql)
    df_with_labels.createOrReplaceTempView("df_with_labels")

    sql = """
    select truth_threshold, count(*) as num_records_in_row, sum(c_P) as c_P, sum(c_N) as c_N
    from
    df_with_labels
    group by truth_threshold
    order by truth_threshold
    """
    df_with_labels_grouped = spark.sql(sql)
    df_with_labels_grouped.createOrReplaceTempView("df_with_labels_grouped")

    sql = """
    select
    truth_threshold,

    (sum(c_P) over (order by truth_threshold desc))  as cum_clerical_P,
    (sum(c_N) over (order by truth_threshold)) - c_N as cum_clerical_N,

    (select sum(c_P) from df_with_labels_grouped) as total_clerical_P,
    (select sum(c_N) from df_with_labels_grouped) as total_clerical_N,
    (select sum(num_records_in_row) from df_with_labels_grouped) as row_count,

    -num_records_in_row + sum(num_records_in_row) over (order by truth_threshold) as N_labels,
    sum(num_records_in_row) over (order by truth_threshold desc) as P_labels
    from df_with_labels_grouped
    order by  truth_threshold
    """
    df_with_cumulative_labels = spark.sql(sql)
    df_with_cumulative_labels.createOrReplaceTempView("df_with_cumulative_labels")

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

    from df_with_cumulative_labels
    """
    df_with_truth_cats = spark.sql(sql)
    df_with_truth_cats.createOrReplaceTempView("df_with_truth_cats")
    df_with_truth_cats.toPandas()

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
        N/row_count as N_rate,
        TP/P as TP_rate,
        TN/N as TN_rate,
        FP/N as FP_rate,
        FN/P as FN_rate,
        TP/(TP+FP) as precision,
        TP/(TP+FN) as recall
    from df_with_truth_cats
    """
    df_truth_space = spark.sql(sql)

    return df_truth_space


def roc_chart(
    df_labels_with_splink_scores: Union[DataFrame, dict],
    spark: SparkSession,
    threshold_actual: float = 0.5,
    x_domain: list = None,
    width: int = 400,
    height: int = 400,
):
    """Create a ROC chart from labelled data

    Args:
        df_labels_with_splink_scores (Union[DataFrame, dict]): A dataframe of labels and associated splink scores
            usually the output of the truth.labels_with_splink_scores function.  Or, a dict containing
            one such dataframe per key.  {'model 1': df1, 'model 2': df2}.  If a dict is provided, the
            ROC charts for each model will be plotted on the same figure.
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        x_domain (list, optional): Domain for x axis. Defaults to None.
        width (int, optional):  Defaults to 400.
        height (int, optional):  Defaults to 400.

    """

    roc_chart_def = {
        "config": {"view": {"continuousWidth": 400, "continuousHeight": 300}},
        "data": {"name": "data-fadd0e93e9546856cbc745a99e65285d", "values": None},
        "mark": {"type": "line", "clip": True, "point": True},
        "encoding": {
            "tooltip": [
                {"type": "quantitative", "field": "truth_threshold"},
                {"type": "quantitative", "field": "FP_rate"},
                {"type": "quantitative", "field": "TP_rate"},
                {"type": "quantitative", "field": "TP"},
                {"type": "quantitative", "field": "TN"},
                {"type": "quantitative", "field": "FP"},
                {"type": "quantitative", "field": "FN"},
                {"type": "quantitative", "field": "precision"},
                {"type": "quantitative", "field": "recall"},
            ],
            "x": {
                "type": "quantitative",
                "field": "FP_rate",
                "sort": ["truth_threshold"],
                "title": "False Positive Rate amongst clerically reviewed records",
            },
            "y": {
                "type": "quantitative",
                "field": "TP_rate",
                "sort": ["truth_threshold"],
                "title": "True Positive Rate amongst clerically reviewed records",
            },
            "color": {
                "type": "nominal",
                "field": "roc_label",
            },
        },
        "selection": {
            "selector076": {
                "type": "interval",
                "bind": "scales",
                "encodings": ["x"],
            }
        },
        "height": height,
        "title": "Receiver operating characteristic curve",
        "width": width,
    }

    if type(df_labels_with_splink_scores) == pyspark.sql.DataFrame:
        del roc_chart_def["encoding"]["color"]
        df_labels_with_splink_scores = {"model1": df_labels_with_splink_scores}

    dfs = []
    for key, df in df_labels_with_splink_scores.items():
        data = truth_space_table(
            df, spark, threshold_actual=threshold_actual
        ).toPandas()
        data["roc_label"] = key

        dfs.append(data)

    if not x_domain:

        f1 = data["FP_rate"] < 1.0
        filtered = data[f1]
        d1 = filtered["FP_rate"].max() * 1.5

        x_domain = [0, d1]

    roc_chart_def["encoding"]["x"]["scale"] = {"domain": x_domain}

    records = []
    for df in dfs:
        recs = df.to_dict(orient="records")
        records.extend(recs)

    roc_chart_def["data"]["values"] = records

    if altair_installed:
        return alt.Chart.from_dict(roc_chart_def)
    else:
        return roc_chart_def


def precision_recall_chart(
    df_labels_with_splink_scores,
    spark,
    threshold_actual=0.5,
    domain=None,
    width=400,
    height=400,
):
    """Create a precision recall chart from labelled data

    Args:
        df_labels_with_splink_scores (DataFrame): A dataframe of labels and associated splink scores
            usually the output of the truth.labels_with_splink_scores function
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        x_domain (list, optional): Domain for x axis. Defaults to None.
        width (int, optional):  Defaults to 400.
        height (int, optional):  Defaults to 400.

    """

    pr_chart_def = {
        "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
        "config": {"view": {"continuousWidth": 400, "continuousHeight": 300}},
        "data": {"name": "data-fadd0e93e9546856cbc745a99e65285d", "values": None},
        "mark": {"type": "line", "clip": True, "point": True},
        "encoding": {
            "tooltip": [
                {"type": "quantitative", "field": "truth_threshold"},
                {"type": "quantitative", "field": "FP_rate"},
                {"type": "quantitative", "field": "TP_rate"},
                {"type": "quantitative", "field": "TP"},
                {"type": "quantitative", "field": "TN"},
                {"type": "quantitative", "field": "FP"},
                {"type": "quantitative", "field": "FN"},
                {"type": "quantitative", "field": "precision"},
                {"type": "quantitative", "field": "recall"},
            ],
            "x": {
                "type": "quantitative",
                "field": "recall",
                "sort": ["-recall"],
                "title": "Recall",
            },
            "y": {
                "type": "quantitative",
                "field": "precision",
                "sort": ["-precision"],
                "title": "Precision",
            },
        },
        "height": height,
        "title": "Precision-recall curve",
        "width": width,
    }

    if domain:
        pr_chart_def["encoding"]["x"]["scale"]["domain"] = domain

    data = truth_space_table(
        df_labels_with_splink_scores, spark, threshold_actual=threshold_actual
    ).toPandas()

    data = data.to_dict(orient="records")

    pr_chart_def["data"]["values"] = data

    if altair_installed:
        return alt.Chart.from_dict(pr_chart_def)
    else:
        return pr_chart_def

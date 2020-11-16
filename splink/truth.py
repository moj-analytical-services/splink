from .settings import complete_settings_dict

from functools import reduce
from pyspark.sql import DataFrame

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False

try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None


def _sql_gen_unique_id_keygen(table:str, uid_col1:str, uid_col2:str):
    """Create a composite unique id for a pairwise comparisons
    This is a concatenation of the unique id of each record
    of the pairwise comparison.

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

    Returns:
        str: sql case expression that outputs the composite unique_id
    """

    return f"""
    case
    when {table}.{uid_col1} > {table}.{uid_col2} then concat({table}.{uid_col2}, '-', {table}.{uid_col1})
    else concat({table}.{uid_col1}, '-', {table}.{uid_col2})
    end
    """


def _check_df_labels(df_labels, settings):
    """Check the df_labels provided contains the expected columns
    """

    cols = df_labels.columns
    colname = settings["unique_id_column_name"]

    assert f"{colname}_l" in cols, f"{colname}_l should be a column in df_labels"
    assert f"{colname}_r" in cols, f"{colname}_l should be a column in df_labels"
    assert (
        "clerical_match_score" in cols
    ), f"clerical_match_score should be a column in df_labels"


def _get_score_colname(settings):
    score_colname = "match_probability"
    for c in settings["comparison_columns"]:
        if c["term_frequency_adjustments"] is True:
            score_colname = "tf_adjusted_match_prob"
    return score_colname


def _join_labels_to_results(df_labels, df_e, settings, spark):

    # df_labels is a dataframe like:
    # | unique_id_l | unique_id_r | clerical_match_score |
    # |:------------|:------------|---------------------:|
    # | id1         | id2         |                  0.9 |
    # | id1         | id3         |                  0.1 |

    # df_e is a dataframe like
    # | unique_id_l| unique_id_r| tf_adjusted_match_prob |
    # |:-----------|:-----------|-----------------------:|
    # | id1        | id2        |                   0.85 |
    # | id1        | id3        |                   0.2  |
    # | id2        | id3        |                   0.1  |
    settings = complete_settings_dict(settings, None)

    _check_df_labels(df_labels, settings)

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
    df_e_with_labels, threshold_pred, settings, spark, threshold_actual=0.5
):
    """Take a dataframe with clerical labels and splink predictions and
    label each row with truth categories (true positive, true negative etc)
    """

    # df_e_with_labels is a dataframe like
    # |     unique_id_l   | unique_id_r   |   clerical_match_score |   tf_adjusted_match_prob |
    # |:------------------|:--------------|-----------------------:|-------------------------:|
    # | id1               | id2           |                    0.9 |                     0.85 |
    # | id1               | id3           |                    0.1 |                     0.2  |


    df_e_with_labels.createOrReplaceTempView("df_e_with_labels")

    score_colname = _get_score_colname(settings)

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
    df_labels: DataFrame,
    df_e: DataFrame,
    settings: dict,
    threshold_pred: float,
    spark: SparkSession,
    threshold_actual: float = 0.5,
):
    """Join Splink's predictions to clerically labelled data and categorise
    rows by truth category (false positive, true positive etc.)

    Note that df_labels

    Args:
        df_labels (DataFrame): A dataframe of clerically labelled data
            with ids that match the unique_id_column sepcified in the
            splink settings object.  If the column is called unique_id
            df_labels should look like:
            | unique_id_l | unique_id_r | clerical_match_score |
            |:------------|:------------|---------------------:|
            | id1         | id2         |                  0.9 |
            | id1         | id3         |                  0.1 |
        df_e (DataFrame): Splink output of scored pairwise record comparisons
            | unique_id_l| unique_id_r| tf_adjusted_match_prob |
            |:-----------|:-----------|-----------------------:|
            | id1        | id2        |                   0.85 |
            | id1        | id3        |                   0.2  |
            | id2        | id3        |                   0.1  |
        settings (dict): splink settings dictionary
        threshold_pred (float): Threshold to use in categorising Splink predictions into
            match or no match
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.

    Returns:
        DataFrame: Dataframe of labels associated with truth category
    """
    df_labels = _join_labels_to_results(df_labels, df_e, settings, spark)
    df_e_t = _categorise_scores_into_truth_cats(
        df_labels, threshold_pred, settings, spark, threshold_actual
    )
    return df_e_t


def truth_space_table(
    df_labels: DataFrame,
    df_e: DataFrame,
    settings: dict,
    spark: SparkSession,
    threshold_actual: float = 0.5,
):
    """Create a table of the ROC space i.e. truth table statistics
    for each discrimination threshold

    Args:
        df_labels (DataFrame): A dataframe of clerically labelled data
            with ids that match the unique_id_column sepcified in the
            splink settings object.  If the column is called unique_id
            df_labels should look like:
            | unique_id_l | unique_id_r | clerical_match_score |
            |:------------|:------------|---------------------:|
            | id1         | id2         |                  0.9 |
            | id1         | id3         |                  0.1 |
        df_e (DataFrame): Splink output of scored pairwise record comparisons
            | unique_id_l| unique_id_r| tf_adjusted_match_prob |
            |:-----------|:-----------|-----------------------:|
            | id1        | id2        |                   0.85 |
            | id1        | id3        |                   0.2  |
            | id2        | id3        |                   0.1  |
        settings (dict): splink settings dictionary
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.

    Returns:
        DataFrame: Table of 'truth space' i.e. truth categories for each threshold level
    """

    df_labels_results = _join_labels_to_results(df_labels, df_e, settings, spark)

    # This is used repeatedly to generate the roc curve
    df_labels_results.persist()

    # We want percentiles of score to compute
    score_colname = _get_score_colname(settings)

    percentiles = [x / 100 for x in range(0, 101)]

    values_distinct = df_labels_results.select(score_colname).distinct()
    thresholds = values_distinct.stat.approxQuantile(score_colname, percentiles, 0.0)
    thresholds.append(1.01)
    thresholds = sorted(set(thresholds))

    roc_dfs = []
    for thres in thresholds:
        df_e_t = _categorise_scores_into_truth_cats(
            df_labels_results, thres, settings, spark, threshold_actual
        )
        df_roc_row = _summarise_truth_cats(df_e_t, spark)
        roc_dfs.append(df_roc_row)

    all_roc_df = reduce(DataFrame.unionAll, roc_dfs)
    return all_roc_df


def roc_chart(
    df_labels: DataFrame,
    df_e: DataFrame,
    settings: dict,
    spark: SparkSession,
    threshold_actual: float = 0.5,
    x_domain: list = None,
    width: int = 400,
    height: int = 400,
):
    """Create a ROC chart from labelled data

    Args:
        df_labels (DataFrame): A dataframe of clerically labelled data
            with ids that match the unique_id_column sepcified in the
            splink settings object.  If the column is called unique_id
            df_labels should look like:
            | unique_id_l | unique_id_r | clerical_match_score |
            |:------------|:------------|---------------------:|
            | id1         | id2         |                  0.9 |
            | id1         | id3         |                  0.1 |
        df_e (DataFrame): Splink output of scored pairwise record comparisons
            | unique_id_l| unique_id_r| tf_adjusted_match_prob |
            |:-----------|:-----------|-----------------------:|
            | id1        | id2        |                   0.85 |
            | id1        | id3        |                   0.2  |
            | id2        | id3        |                   0.1  |
        settings (dict): splink settings dictionary
        spark (SparkSession): SparkSession object
        threshold_actual (float, optional): Threshold to use in categorising clerical match
            scores into match or no match. Defaults to 0.5.
        x_domain (list, optional): Domain for x axis. Defaults to None.
        width (int, optional):  Defaults to 400.
        height (int, optional):  Defaults to 400.

    """

    roc_chart_def = {
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
        },
        "height": height,
        "title": "Receiver operating characteristic curve",
        "width": width,
    }

    data = truth_space_table(
        df_labels, df_e, settings, spark, threshold_actual=threshold_actual
    ).toPandas()

    if not x_domain:

        f1 = data["FP_rate"] < 1.0
        filtered = data[f1]
        d1 = filtered["FP_rate"].max() * 1.5

        x_domain = [0, d1]

    roc_chart_def["encoding"]["x"]["scale"] = {"domain": x_domain}

    data = data.to_dict(orient="rows")

    roc_chart_def["data"]["values"] = data

    if altair_installed:
        return alt.Chart.from_dict(roc_chart_def)
    else:
        return roc_chart_def


def precision_recall_chart(
    df_labels,
    df_e,
    settings,
    spark,
    threshold_actual=0.5,
    domain=None,
    width=400,
    height=400,
):
    """Create a precision recall chart from labelled data

    Args:
        df_labels (DataFrame): A dataframe of clerically labelled data
            with ids that match the unique_id_column sepcified in the
            splink settings object.  If the column is called unique_id
            df_labels should look like:
            | unique_id_l | unique_id_r | clerical_match_score |
            |:------------|:------------|---------------------:|
            | id1         | id2         |                  0.9 |
            | id1         | id3         |                  0.1 |
        df_e (DataFrame): Splink output of scored pairwise record comparisons
            | unique_id_l| unique_id_r| tf_adjusted_match_prob |
            |:-----------|:-----------|-----------------------:|
            | id1        | id2        |                   0.85 |
            | id1        | id3        |                   0.2  |
            | id2        | id3        |                   0.1  |
        settings (dict): splink settings dictionary
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
        df_labels, df_e, settings, spark, threshold_actual=threshold_actual
    ).toPandas()

    data = data.to_dict(orient="rows")

    pr_chart_def["data"]["values"] = data

    if altair_installed:
        return alt.Chart.from_dict(pr_chart_def)
    else:
        return pr_chart_def

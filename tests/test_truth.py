import logging

logger = logging.getLogger(__name__)
from pyspark.sql import Row

from splink.truth import (
    labels_with_splink_scores,
    df_e_with_truth_categories,
    truth_space_table,
    roc_chart,
    precision_recall_chart,
    dedupe_splink_scores,
)

import pytest


def test_roc_1(spark):

    df_e = [
        {"person_id_l": 1, "person_id_r": 11, "match_probability": 0.0},
        {"person_id_l": 2, "person_id_r": 12, "match_probability": 0.4},
        {"person_id_l": 3, "person_id_r": 13, "match_probability": 1.0},
        {"person_id_l": 4, "person_id_r": 14, "match_probability": 1.0},
    ]
    df_e = spark.createDataFrame(Row(**x) for x in df_e)

    df_labels = [
        {"person_id_l": 1, "person_id_r": 11, "clerical_match_score": 0.1},
        {"person_id_l": 2, "person_id_r": 12, "clerical_match_score": 0.45},
        {"person_id_l": 3, "person_id_r": 13, "clerical_match_score": 0.01},
    ]
    df_labels = spark.createDataFrame(Row(**x) for x in df_labels)

    df_labels_with_splink_scores = labels_with_splink_scores(
        df_labels, df_e, "person_id", spark
    )

    df_truth = df_e_with_truth_categories(df_labels_with_splink_scores, 0.5, spark)

    df_truth = df_truth.toPandas()

    f1 = df_truth["person_id_l"] == 1
    f2 = df_truth["person_id_r"] == 11

    row = df_truth[f1 & f2].to_dict(orient="records")[0]

    assert row["P"] is False
    assert row["N"] is True
    assert row["TN"] is True

    f1 = df_truth["person_id_l"] == 3
    f2 = df_truth["person_id_r"] == 13

    row = df_truth[f1 & f2].to_dict(orient="records")[0]

    assert row["P"] is False
    assert row["N"] is True
    assert row["TP"] is False
    assert row["FP"] is True

    df_roc = truth_space_table(df_labels_with_splink_scores, spark)
    df_roc = df_roc.toPandas()

    # Note that our critiera are great than or equal to
    # meaning match prob of 0.40 is treated as a match at threshold 0.40
    f1 = df_roc["truth_threshold"] > 0.39
    f2 = df_roc["truth_threshold"] < 0.41

    row = df_roc[f1 & f2].to_dict(orient="records")[0]

    # FPR = FP/(FP+TN) = FP/N
    # At 0.4 we have
    # 1,11 is a TN
    # 2,12 is a P at 40% against a clerical N (45%) so is a FP
    # 3,13 is a FP as well

    assert pytest.approx(row["FP_rate"]) == 2 / 3

    # Precision = TP/TP+FP
    assert row["precision"] == 0.0

    # Check no errors from charting functions

    roc_chart(df_labels_with_splink_scores, spark)
    precision_recall_chart(df_labels_with_splink_scores, spark)


def test_join(spark):

    df_e = [
        {
            "source_dataset_l": "t1",
            "person_id_l": 1,
            "source_dataset_r": "t2",
            "person_id_r": 1,
            "tf_adjusted_match_prob": 0.0,
        },
        {
            "source_dataset_l": "t1",
            "person_id_l": 1,
            "source_dataset_r": "t2",
            "person_id_r": 2,
            "tf_adjusted_match_prob": 0.4,
        },
        {
            "source_dataset_l": "t2",
            "person_id_l": 1,
            "source_dataset_r": "t1",
            "person_id_r": 2,
            "tf_adjusted_match_prob": 1.0,
        },
    ]
    df_e = spark.createDataFrame(Row(**x) for x in df_e)

    df_labels = [
        {
            "source_dataset_l": "t2",
            "person_id_l": 1,
            "source_dataset_r": "t1",
            "person_id_r": 1,
            "clerical_match_score": 0.1,
        },
        {
            "source_dataset_l": "t2",
            "person_id_l": 1,
            "source_dataset_r": "t1",
            "person_id_r": 2,
            "clerical_match_score": 0.45,
        },
        {
            "source_dataset_l": "t1",
            "person_id_l": 1,
            "source_dataset_r": "t2",
            "person_id_r": 2,
            "clerical_match_score": 0.01,
        },
    ]
    df_labels = spark.createDataFrame(Row(**x) for x in df_labels)

    df_labels_with_splink_scores = labels_with_splink_scores(
        df_labels, df_e, "person_id", spark, join_on_source_dataset=True
    )

    df_pd = df_labels_with_splink_scores.toPandas()

    f1 = df_pd["source_dataset_l"] == "t1"
    f2 = df_pd["person_id_l"] == 1
    f3 = df_pd["source_dataset_r"] == "t2"
    f4 = df_pd["person_id_r"] == 1

    row = df_pd[f1 & f2 & f3 & f4].to_dict(orient="records")[0]

    assert pytest.approx(row["tf_adjusted_match_prob"]) == 0.0
    assert pytest.approx(row["clerical_match_score"]) == 0.1

    f1 = df_pd["source_dataset_l"] == "t2"
    f2 = df_pd["person_id_l"] == 1
    f3 = df_pd["source_dataset_r"] == "t1"
    f4 = df_pd["person_id_r"] == 2

    row = df_pd[f1 & f2 & f3 & f4].to_dict(orient="records")[0]

    assert pytest.approx(row["tf_adjusted_match_prob"]) == 1.0
    assert pytest.approx(row["clerical_match_score"]) == 0.45


def test_dedupe(spark):

    df_e = [
        {"person_id_l": 1, "person_id_r": 2, "match_probability": 0.1},
        {"person_id_l": 1, "person_id_r": 2, "match_probability": 0.8},
        {"person_id_l": 1, "person_id_r": 2, "match_probability": 0.3},
        {"person_id_l": 1, "person_id_r": 4, "match_probability": 1.0},
        {"person_id_l": 5, "person_id_r": 6, "match_probability": 0.0},
    ]
    df_e = spark.createDataFrame(Row(**x) for x in df_e)

    df = dedupe_splink_scores(df_e, "person_id", selection_fn="abs_val")

    df_pd = df.toPandas()

    f1 = df_pd["person_id_l"] == 1
    f2 = df_pd["person_id_r"] == 2

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert row["match_probability"] == 0.1

    f1 = df_pd["person_id_l"] == 1
    f2 = df_pd["person_id_r"] == 4

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert row["match_probability"] == 1.0

    f1 = df_pd["person_id_l"] == 5
    f2 = df_pd["person_id_r"] == 6

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert row["match_probability"] == 0.0

    df = dedupe_splink_scores(df_e, "person_id", selection_fn="mean")

    df_pd = df.toPandas()

    f1 = df_pd["person_id_l"] == 1
    f2 = df_pd["person_id_r"] == 2

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert pytest.approx(row["match_probability"]) == 0.4

    f1 = df_pd["person_id_l"] == 1
    f2 = df_pd["person_id_r"] == 4

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert row["match_probability"] == 1.0

    f1 = df_pd["person_id_l"] == 5
    f2 = df_pd["person_id_r"] == 6

    row = df_pd[f1 & f2].to_dict(orient="records")[0]

    assert row["match_probability"] == 0.0

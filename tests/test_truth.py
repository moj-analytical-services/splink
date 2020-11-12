import logging

logger = logging.getLogger(__name__)
from pyspark.sql import Row

from splink.truth import (
    df_e_with_truth_categories,
    truth_space_table,
    roc_chart,
    precision_recall_chart,
)

import pytest


def test_roc(spark):

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "person_id",
        "comparison_columns": [{"col_name": "col1"}],
    }

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

    df_truth = df_e_with_truth_categories(df_labels, df_e, settings, 0.5, spark)

    df_truth = df_truth.toPandas()

    f1 = df_truth["person_id_l"] == 1
    f2 = df_truth["person_id_r"] == 11

    row = df_truth[f1 & f2].to_dict(orient="rows")[0]

    assert row["P"] is False
    assert row["N"] is True
    assert row["TN"] is True

    f1 = df_truth["person_id_l"] == 3
    f2 = df_truth["person_id_r"] == 13

    row = df_truth[f1 & f2].to_dict(orient="rows")[0]

    assert row["P"] is False
    assert row["N"] is True
    assert row["TP"] is False
    assert row["FP"] is True

    df_roc = truth_space_table(df_labels, df_e, settings, spark)
    df_roc = df_roc.toPandas()

    # Note that our critiera are great than or equal to
    # meaning match prob of 0.40 is treated as a match at threshold 0.40
    f1 = df_roc["truth_threshold"] > 0.39
    f2 = df_roc["truth_threshold"] < 0.41

    row = df_roc[f1 & f2].to_dict(orient="rows")[0]

    # FPR = FP/(FP+TN) = FP/N
    # At 0.4 we have
    # 1,11 is a TN
    # 2,12 is a P at 40% against a clerical N (45%) so is a FP
    # 3,13 is a FP as well

    assert pytest.approx(row["FP_rate"]) == 2 / 3

    # Precision = TP/TP+FP
    assert row["precision"] == 0.0

    # Check no errors from charting functions

    df_roc = truth_space_table(df_labels, df_e, settings, spark)
    roc_chart(df_labels, df_e, settings, spark)
    precision_recall_chart(df_labels, df_e, settings, spark)
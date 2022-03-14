import pandas as pd
import pytest
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.accuracy import (
    predict_scores_for_labels,
    truth_space_table_from_labels_with_predictions,
)
from splink.comparison_library import exact_match

from basic_settings import settings_dict


def test_scored_labels():

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.head(5)
    labels = [
        (0, 1, 0.8),
        (2, 0, 0.9),
        (0, 3, 0.95),
        (1, 2, 1.0),
        (3, 1, 1.0),
        (3, 2, 1.0),
    ]

    df_labels = pd.DataFrame(
        labels, columns=["unique_id_l", "unique_id_r", "clerical_match_score"]
    )
    df_labels["source_dataset_l"] = "fake_data_1"
    df_labels["source_dataset_r"] = "fake_data_1"

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            exact_match("first_name"),
            exact_match("surname"),
            exact_match("dob"),
        ],
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
            "l.dob = r.dob",
        ],
    }

    linker = DuckDBLinker(settings, input_tables={"fake_data_1": df})

    linker._initialise_df_concat_with_tf()
    linker.con.register("labels", df_labels)

    df_scores_labels = predict_scores_for_labels(linker, "labels")
    df_scores_labels = df_scores_labels.as_pandas_dataframe()
    df_scores_labels.sort_values(["unique_id_l", "unique_id_r"], inplace=True)

    assert len(df_scores_labels) == 6

    # Check predictions are the same as the labels
    df_predict = linker.predict().as_pandas_dataframe()

    f1 = df_predict["unique_id_l"] == 1
    f2 = df_predict["unique_id_r"] == 2
    predict_weight = df_predict.loc[f1 & f2, "match_weight"]

    f1 = df_scores_labels["unique_id_l"] == 1
    f2 = df_scores_labels["unique_id_r"] == 2
    labels_weight = df_scores_labels.loc[f1 & f2, "match_weight"]

    assert pytest.approx(predict_weight) == labels_weight

    # Test 0 vs 1 has found_by_blocking_rules false
    f1 = df_scores_labels["unique_id_l"] == 0
    f2 = df_scores_labels["unique_id_r"] == 1
    val = df_scores_labels.loc[f1 & f2, "found_by_blocking_rules"].iloc[0]
    assert bool(val) is False

    # Test 1 vs 2 has found_by_blocking_rules false
    f1 = df_scores_labels["unique_id_l"] == 1
    f2 = df_scores_labels["unique_id_r"] == 2
    val = df_scores_labels.loc[f1 & f2, "found_by_blocking_rules"].iloc[0]
    assert bool(val) is True


def test_truth_space_table():

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            exact_match("first_name"),
            exact_match("surname"),
            exact_match("dob"),
        ],
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
            "l.dob = r.dob",
        ],
    }

    linker = DuckDBLinker(settings, input_tables={"fake_data_1": df})

    labels_with_predictions = [
        {
            "person_id_l": 1,
            "person_id_r": 11,
            "match_weight": 0.0,
            "found_by_blocking_rules": False,
            "clerical_match_score": 0.1,
        },
        {
            "person_id_l": 2,
            "person_id_r": 12,
            "match_weight": 0.4,
            "found_by_blocking_rules": False,
            "clerical_match_score": 0.45,
        },
        {
            "person_id_l": 3,
            "person_id_r": 13,
            "match_weight": 1.0,
            "found_by_blocking_rules": False,
            "clerical_match_score": 0.01,
        },
    ]
    labels_with_predictions = pd.DataFrame(labels_with_predictions)

    linker.con.register("__splink__labels_with_predictions", labels_with_predictions)

    sqls = truth_space_table_from_labels_with_predictions(0.5)
    for sql in sqls:
        linker.enqueue_sql(sql["sql"], sql["output_table_name"])
    df_roc = linker.execute_sql_pipeline()

    df_roc = df_roc.as_pandas_dataframe()

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


def test_roc_chart():

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(10)

    df["source_dataset"] = "fake_data_1"
    df["merge"] = 1

    df_l = df[["unique_id", "source_dataset", "group", "merge"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="merge", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1]

    df_labels["clerical_match_score"] = (
        df_labels["group_l"] == df_labels["group_r"]
    ).astype(float)

    linker = DuckDBLinker(
        settings_dict, input_tables={"fake_data_1": df}, connection=":memory:"
    )

    linker._initialise_df_concat_with_tf()

    linker.con.register("labels", df_labels)

    linker.roc_from_labels("labels")

import pandas as pd
import pytest
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.accuracy import predict_scores_for_labels
from splink.comparison_library import exact_match


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

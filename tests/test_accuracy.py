import duckdb
import pandas as pd
import pytest

from splink import SettingsCreator
from splink.internals.accuracy import (
    predictions_from_sample_of_pairwise_labels_sql,
    truth_space_table_from_labels_with_predictions_sqls,
)
from splink.internals.blocking_rule_library import block_on
from splink.internals.comparison_library import ExactMatch
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from .basic_settings import get_settings_dict


def test_scored_labels_table():
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
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
            block_on("dob"),
        ],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    pipeline = CTEPipeline()
    concat_with_tf = compute_df_concat_with_tf(linker, pipeline)

    pipeline = CTEPipeline([concat_with_tf])
    linker.table_management.register_table(df_labels, "labels")

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, "labels")
    pipeline.enqueue_list_of_sqls(sqls)

    df_scores_labels = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    df_scores_labels = df_scores_labels.as_pandas_dataframe()
    df_scores_labels.sort_values(["unique_id_l", "unique_id_r"], inplace=True)

    assert len(df_scores_labels) == 6

    # Check predictions are the same as the labels
    df_predict = linker.inference.predict().as_pandas_dataframe()

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
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
            block_on("dob"),
        ],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    labels_with_predictions = [
        {
            "person_id_l": 1,
            "person_id_r": 11,
            "match_weight": 0.0,
            "found_by_blocking_rules": True,
            "clerical_match_score": 0.1,
        },
        {
            "person_id_l": 2,
            "person_id_r": 12,
            "match_weight": 0.4,
            "found_by_blocking_rules": True,
            "clerical_match_score": 0.45,
        },
        {
            "person_id_l": 3,
            "person_id_r": 13,
            "match_weight": 1.0,
            "found_by_blocking_rules": True,
            "clerical_match_score": 0.01,
        },
    ]
    labels_with_predictions = pd.DataFrame(labels_with_predictions)

    linker.table_management.register_table(
        labels_with_predictions, "__splink__labels_with_predictions"
    )
    pipeline = CTEPipeline()
    sqls = truth_space_table_from_labels_with_predictions_sqls(0.5)
    pipeline.enqueue_list_of_sqls(sqls)

    df_roc = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

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

    assert pytest.approx(row["fp_rate"]) == 2 / 3

    # Precision = TP/TP+FP
    assert row["precision"] == 0.0


def test_roc_chart_dedupe_only():
    # No source_dataset required in labels

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(10)

    df["source_dataset"] = "fake_data_1"
    df["merge"] = 1

    df_l = df[["unique_id", "source_dataset", "cluster", "merge"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="merge", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1]

    df_labels["clerical_match_score"] = (
        df_labels["cluster_l"] == df_labels["cluster_r"]
    ).astype(float)

    df_labels = df_labels.drop(
        ["cluster_l", "cluster_r", "source_dataset_l", "source_dataset_r", "merge"],
        axis=1,
    )
    settings_dict = get_settings_dict()
    db_api = DuckDBAPI(connection=":memory:")

    linker = Linker(df, settings_dict, db_api=db_api)

    labels_sdf = linker.table_management.register_table(df_labels, "labels")

    linker.evaluation.accuracy_analysis_from_labels_table(labels_sdf, output_type="roc")


def test_roc_chart_link_and_dedupe():
    # Source dataset required in labels

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(10)

    df["source_dataset"] = "fake_data_1"
    df["merge"] = 1

    df_l = df[["unique_id", "source_dataset", "cluster", "merge"]].copy()
    df_r = df_l.copy()

    df_labels = df_l.merge(df_r, on="merge", suffixes=("_l", "_r"))
    f1 = df_labels["unique_id_l"] < df_labels["unique_id_r"]
    df_labels = df_labels[f1]

    df_labels["clerical_match_score"] = (
        df_labels["cluster_l"] == df_labels["cluster_r"]
    ).astype(float)

    df_labels = df_labels.drop(["cluster_l", "cluster_r", "merge"], axis=1)
    settings_dict = get_settings_dict()
    settings_dict["link_type"] = "link_and_dedupe"
    db_api = DuckDBAPI(connection=":memory:")

    linker = Linker(df, settings_dict, input_table_aliases="fake_data_1", db_api=db_api)

    labels_sdf = linker.table_management.register_table(df_labels, "labels")

    linker.evaluation.accuracy_analysis_from_labels_table(labels_sdf, output_type="roc")


def test_prediction_errors_from_labels_table():
    data = [
        {"unique_id": 1, "first_name": "robin", "cluster": 1},
        {"unique_id": 2, "first_name": "robin", "cluster": 1},
        {"unique_id": 3, "first_name": "john", "cluster": 1},
        {"unique_id": 4, "first_name": "david", "cluster": 2},
        {"unique_id": 5, "first_name": "david", "cluster": 3},
    ]
    df = pd.DataFrame(data)

    labels = [
        (1, 2, 0.8),
        (1, 3, 0.8),
        (2, 3, 0.8),
        (4, 5, 0.1),
    ]

    df_labels = pd.DataFrame(
        labels, columns=["unique_id_l", "unique_id_r", "clerical_match_score"]
    )
    df_labels["source_dataset_l"] = "fake_data_1"
    df_labels["source_dataset_r"] = "fake_data_1"

    sql = '"first_name_l" IS NULL OR "first_name_r" IS NULL'
    settings = {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.5,
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": sql,
                        "label_for_charts": "Null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": '"first_name_l" = "first_name_r"',
                        "label_for_charts": "Exact match",
                        "m_probability": 0.95,
                        "u_probability": 1e-5,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.05,
                        "u_probability": 1 - 1e-5,
                    },
                ],
            }
        ],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table(
        "labels"
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) in records  # fn
    assert (2, 3) in records  # fn
    assert (4, 5) in records  # fp
    assert (1, 2) not in records  # tp

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table(
        "labels", include_false_negatives=False
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) not in records  # fn
    assert (2, 3) not in records  # fn
    assert (4, 5) in records  # fp
    assert (1, 2) not in records  # tp

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table(
        "labels", include_false_positives=False
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) in records  # fn
    assert (2, 3) in records  # fn
    assert (4, 5) not in records  # fp
    assert (1, 2) not in records  # tp


def test_prediction_errors_from_labels_column():
    data = [
        {"unique_id": 1, "first_name": "robin", "cluster": 1},
        {"unique_id": 2, "first_name": "robin", "cluster": 1},
        {"unique_id": 3, "first_name": "john", "cluster": 1},
        {"unique_id": 4, "first_name": "david", "cluster": 2},
        {"unique_id": 5, "first_name": "david", "cluster": 3},
    ]
    df = pd.DataFrame(data)

    sql = '"first_name_l" IS NULL OR "first_name_r" IS NULL'
    settings = {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.5,
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": sql,
                        "label_for_charts": "Null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": '"first_name_l" = "first_name_r"',
                        "label_for_charts": "Exact match",
                        "m_probability": 0.95,
                        "u_probability": 1e-5,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.05,
                        "u_probability": 1 - 1e-5,
                    },
                ],
            }
        ],
        "blocking_rules_to_generate_predictions": ["1=1"],
    }

    #
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    df_res = linker.evaluation.prediction_errors_from_labels_column(
        "cluster"
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) in records  # FN
    assert (2, 3) in records  # FN
    assert (4, 5) in records  # FP
    assert (1, 2) not in records  # TP
    assert (1, 5) not in records  # TN

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    df_res = linker.evaluation.prediction_errors_from_labels_column(
        "cluster", include_false_positives=False
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) in records  # FN
    assert (2, 3) in records  # FN
    assert (4, 5) not in records  # FP
    assert (1, 2) not in records  # TP
    assert (1, 5) not in records  # TN

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    df_res = linker.evaluation.prediction_errors_from_labels_column(
        "cluster", include_false_negatives=False
    ).as_pandas_dataframe()
    df_res = df_res[["unique_id_l", "unique_id_r"]]
    records = list(df_res.to_records(index=False))
    records = [tuple(p) for p in records]

    assert (1, 3) not in records  # FN
    assert (2, 3) not in records  # FN
    assert (4, 5) in records  # FP
    assert (1, 2) not in records  # TP
    assert (1, 5) not in records  # TN


def test_truth_space_table_from_labels_column_dedupe_only():
    data = [
        {"unique_id": 1, "first_name": "john", "cluster": 1},
        {"unique_id": 2, "first_name": "john", "cluster": 1},
        {"unique_id": 3, "first_name": "john", "cluster": 1},
        {"unique_id": 4, "first_name": "john", "cluster": 2},
        {"unique_id": 5, "first_name": "edith", "cluster": 3},
        {"unique_id": 6, "first_name": "mary", "cluster": 3},
    ]

    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.5,
        "blocking_rules_to_generate_predictions": [
            "1=1",
        ],
        "comparisons": [
            {
                "output_column_name": "First name",
                "comparison_levels": [
                    {
                        "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": "first_name_l = first_name_r",
                        "label_for_charts": "Exact match",
                        "m_probability": 0.9,
                        "u_probability": 0.1,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.1,
                        "u_probability": 0.9,
                    },
                ],
            },
        ],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    tt = linker.evaluation.accuracy_analysis_from_labels_column(
        "cluster", output_type="table"
    ).as_record_dict()
    # Truth threshold -3.17, meaning all comparisons get classified as positive
    truth_dict = tt[0]
    assert truth_dict["tp"] == 4
    assert truth_dict["fp"] == 11
    assert truth_dict["tn"] == 0
    assert truth_dict["fn"] == 0

    # Truth threshold 3.17, meaning only comparisons where forename match get classified
    # as positive
    truth_dict = tt[1]
    assert truth_dict["tp"] == 3
    assert truth_dict["fp"] == 3
    assert truth_dict["tn"] == 8
    assert truth_dict["fn"] == 1


def test_truth_space_table_from_labels_column_link_only():
    data_left = [
        {"unique_id": 1, "first_name": "john", "ground_truth": 1},
        {"unique_id": 2, "first_name": "mary", "ground_truth": 2},
        {"unique_id": 3, "first_name": "edith", "ground_truth": 3},
    ]

    data_right = [
        {"unique_id": 1, "first_name": "john", "ground_truth": 1},
        {"unique_id": 2, "first_name": "john", "ground_truth": 2},
        {"unique_id": 3, "first_name": "eve", "ground_truth": 3},
    ]

    df_left = pd.DataFrame(data_left)
    df_right = pd.DataFrame(data_right)

    settings = {
        "link_type": "link_only",
        "probability_two_random_records_match": 0.5,
        "blocking_rules_to_generate_predictions": [
            "1=1",
        ],
        "comparisons": [
            {
                "output_column_name": "First name",
                "comparison_levels": [
                    {
                        "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": True,
                    },
                    {
                        "sql_condition": "first_name_l = first_name_r",
                        "label_for_charts": "Exact match",
                        "m_probability": 0.9,
                        "u_probability": 0.1,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.1,
                        "u_probability": 0.9,
                    },
                ],
            },
        ],
    }

    db_api = DuckDBAPI()

    linker = Linker([df_left, df_right], settings, db_api=db_api)

    tt = linker.evaluation.accuracy_analysis_from_labels_column(
        "ground_truth", output_type="table"
    ).as_record_dict()
    # Truth threshold -3.17, meaning all comparisons get classified as positive
    truth_dict = tt[0]
    assert truth_dict["tp"] == 3
    assert truth_dict["fp"] == 6
    assert truth_dict["tn"] == 0
    assert truth_dict["fn"] == 0

    # Truth threshold 3.17, meaning only comparisons where forename match get classified
    # as positive
    truth_dict = tt[1]
    assert truth_dict["tp"] == 1
    assert truth_dict["fp"] == 1
    assert truth_dict["tn"] == 5
    assert truth_dict["fn"] == 2


def compute_tp_tn_fp_fn(predictions_pd_df, threshold):
    # Note that in Splink, our critiera are great than or equal to
    # meaning match prob of 0.40 is treated as a match at threshold 0.40
    predicted_match = predictions_pd_df["match_weight"] >= threshold
    actual_match = predictions_pd_df["cluster_l"] == predictions_pd_df["cluster_r"]

    TP = (predicted_match & actual_match).sum()
    TN = (~predicted_match & ~actual_match).sum()
    FP = (predicted_match & ~actual_match).sum()
    FN = (~predicted_match & actual_match).sum()

    return TP, TN, FP, FN


def test_truth_space_table_from_column_vs_pandas_implementaiton_inc_unblocked():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.head(50)

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[block_on("surname"), "1=1"],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_predictions = Linker(df, settings, db_api=DuckDBAPI())
    df_predictions_raw = linker_for_predictions.inference.predict()

    # Score all of the positive labels even if not captured by the blocking rules
    # but not score any negative pairwise comaprisons not captured by the blocking rules
    sql = f"""
    SELECT
        case when
            cluster_l != cluster_r and match_key = 1 then -999.0
            else match_weight
        end as match_weight,
        case when
            cluster_l != cluster_r and match_key = 1 then 0
            else match_probability
        end as match_probability,
        unique_id_l,
        unique_id_r,
        cluster_l,
        cluster_r,
        match_key
    from {df_predictions_raw.physical_name}
    """
    df_predictions = linker_for_predictions.misc.query_sql(sql)

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[block_on("surname")],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_splink_answer = Linker(df, settings, db_api=DuckDBAPI())
    df_from_splink = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_column(
            "cluster",
            output_type="table",
            positives_not_captured_by_blocking_rules_scored_as_zero=False,
        ).as_pandas_dataframe()
    )

    for _, splink_row in df_from_splink.iterrows():
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_column_vs_pandas_implementaiton_ex_unblocked():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.head(100)

    df_1 = df[df["unique_id"] % 2 == 0]
    df_2 = df[df["unique_id"] % 2 == 1]

    # Get a dataframe of predictions vs labels from splink
    # Note we need to add the 1-1 blocking rule
    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        # Add an additional blocking rule to ensure in our results we have
        # all labels
        blocking_rules_to_generate_predictions=[block_on("first_name"), "1=1"],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_predictions = Linker([df_1, df_2], settings, db_api=DuckDBAPI())
    df_predictions_raw = linker_for_predictions.inference.predict()

    # When match_key = 1, the record is not really recovered by the blocking rules
    # so its score must be zero.  Want
    sql = f"""
    SELECT
        case when match_key = 1 then -999.0 else match_weight end as match_weight,
        case when match_key = 1 then 0 else match_probability end as match_probability,
        unique_id_l,
        unique_id_r,
        cluster_l,
        cluster_r,
        match_key
    from {df_predictions_raw.physical_name}
    """
    df_predictions = linker_for_predictions.misc.query_sql(sql)

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        # Remove 1=1 blocking rule to get splink's verrsion of the truth space table
        blocking_rules_to_generate_predictions=[block_on("first_name")],
        additional_columns_to_retain=["cluster"],
    )
    linker_for_splink_answer = Linker([df_1, df_2], settings, db_api=DuckDBAPI())

    df_from_splink = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_column(
            "cluster",
            output_type="table",
            positives_not_captured_by_blocking_rules_scored_as_zero=True,
        ).as_pandas_dataframe()
    )

    for _, splink_row in df_from_splink.iterrows():
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        # Make sure you print the values and ueful message if assert fails
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_table_vs_pandas_cartesian():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df

    df_first_50 = df.head(50).copy()
    sql = """
    SELECT
        l.unique_id as unique_id_l,
        r.unique_id as unique_id_r,
        case when l.cluster = r.cluster then 1 else 0 end as clerical_match_score
    from df_first_50 as l
    inner join df_first_50 as r
    on l.unique_id < r.unique_id
    """
    labels_table = duckdb.sql(sql).df()

    # Predictions for this lael
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=["1=1"],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_predictions = Linker(df_first_50, settings, db_api=DuckDBAPI())
    df_predictions = linker_for_predictions.inference.predict().as_pandas_dataframe()

    linker_for_splink_answer = Linker(df, settings, db_api=DuckDBAPI())
    labels_input = linker_for_splink_answer.table_management.register_labels_table(
        labels_table
    )
    df_from_splink = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_table(
            labels_input, output_type="table"
        ).as_pandas_dataframe()
    )

    for _, splink_row in df_from_splink.iterrows():
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_table_vs_pandas_with_blocking():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    df_1 = df[df["unique_id"] % 2 == 0].copy()
    df_1["source_dataset"] = "left"
    df_2 = df[df["unique_id"] % 2 == 1].copy()
    df_2["source_dataset"] = "right"

    df_1_first_50 = df_1.head(50)
    df_2_first_50 = df_2.head(50)

    sql = """
    SELECT
        l.unique_id as unique_id_l,
        r.unique_id as unique_id_r,
        l.source_dataset as source_dataset_l,
        r.source_dataset as source_dataset_r,
        case when l.cluster = r.cluster then 1 else 0 end as clerical_match_score
    from df_1_first_50 as l
    inner join df_2_first_50 as r
    on concat(l.source_dataset,l.unique_id) < concat(r.source_dataset,r.unique_id)
    """
    labels_table = duckdb.sql(sql).df()

    # Predictions for this lael
    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name"), "1=1"],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_predictions = Linker(
        [df_1_first_50, df_2_first_50], settings, db_api=DuckDBAPI()
    )
    df_predictions_raw = linker_for_predictions.inference.predict()
    df_predictions_raw.as_pandas_dataframe()
    sql = f"""
    select
        case when match_key = 1 then -999.0 else match_weight end as match_weight,
        case when match_key = 1 then 0 else match_probability end as match_probability,
        source_dataset_l,
        source_dataset_r,
        unique_id_l,
        unique_id_r,
        cluster_l,
        cluster_r,
    from {df_predictions_raw.physical_name}
    """
    df_predictions = linker_for_predictions.misc.query_sql(sql)

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            ExactMatch("first_name"),
            ExactMatch("surname"),
            ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
        additional_columns_to_retain=["cluster"],
    )

    linker_for_splink_answer = Linker([df_1, df_2], settings, db_api=DuckDBAPI())
    labels_input = linker_for_splink_answer.table_management.register_labels_table(
        labels_table
    )
    df_from_splink = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_table(
            labels_input, output_type="table"
        ).as_pandas_dataframe()
    )

    for _, splink_row in df_from_splink.iterrows():
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"

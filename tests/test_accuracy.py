import duckdb
import pyarrow as pa
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
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from .basic_settings import get_settings_dict


def get_id_pairs_from_splink_dataframe(sdf: SplinkDataFrame) -> list[tuple[int, int]]:
    return [
        tuple((record["unique_id_l"], record["unique_id_r"]))
        for record in sdf.as_record_dict()
    ]


def test_scored_labels_table(fake_1000):
    df = fake_1000.slice(length=5)

    source_dataset_names = {
        "source_dataset_l": "fake_data_1",
        "source_dataset_r": "fake_data_1",
    }
    df_labels = pa.Table.from_pylist(
        [
            {
                "unique_id_l": 0,
                "unique_id_r": 1,
                "clerical_match_score": 0.8,
                **source_dataset_names,
            },
            {
                "unique_id_l": 2,
                "unique_id_r": 0,
                "clerical_match_score": 0.9,
                **source_dataset_names,
            },
            {
                "unique_id_l": 0,
                "unique_id_r": 3,
                "clerical_match_score": 0.95,
                **source_dataset_names,
            },
            {
                "unique_id_l": 1,
                "unique_id_r": 2,
                "clerical_match_score": 1.0,
                **source_dataset_names,
            },
            {
                "unique_id_l": 3,
                "unique_id_r": 1,
                "clerical_match_score": 1.0,
                **source_dataset_names,
            },
            {
                "unique_id_l": 3,
                "unique_id_r": 2,
                "clerical_match_score": 1.0,
                **source_dataset_names,
            },
        ]
    )

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
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    pipeline = CTEPipeline()
    concat_with_tf = compute_df_concat_with_tf(linker, pipeline)

    pipeline = CTEPipeline([concat_with_tf])
    linker.table_management.register_table(df_labels, "labels")

    sqls = predictions_from_sample_of_pairwise_labels_sql(linker, "labels")
    pipeline.enqueue_list_of_sqls(sqls)

    df_scores_labels = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    df_scores_labels = df_scores_labels.as_duckdbpyrelation()
    df_scores_labels = df_scores_labels.order("unique_id_l, unique_id_r")
    scores_labels_records = df_scores_labels.fetchall()

    assert len(scores_labels_records) == 6

    # Check predictions are the same as the labels
    df_predict = linker.inference.predict().as_duckdbpyrelation()

    predict_12_mw = df_predict.filter("unique_id_l == 1 AND unique_id_r == 2").select(
        "match_weight"
    )
    labels_12_mw = df_scores_labels.filter(
        "unique_id_l == 1 AND unique_id_r == 2"
    ).select("match_weight")

    assert pytest.approx(predict_12_mw.fetchone()[0]) == labels_12_mw.fetchone()[0]

    # Test 0 vs 1 has found_by_blocking_rules false
    val = (
        df_scores_labels.filter("unique_id_l == 0 AND unique_id_r == 1")
        .select("found_by_blocking_rules")
        .fetchone()[0]
    )
    assert bool(val) is False

    # Test 1 vs 2 has found_by_blocking_rules false
    val = (
        df_scores_labels.filter("unique_id_l == 1 AND unique_id_r == 2")
        .select("found_by_blocking_rules")
        .fetchone()[0]
    )
    assert bool(val) is True


def test_truth_space_table(fake_1000):
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
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

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
    labels_with_predictions = pa.Table.from_pylist(labels_with_predictions)

    linker.table_management.register_table(
        labels_with_predictions, "__splink__labels_with_predictions"
    )
    pipeline = CTEPipeline()
    sqls = truth_space_table_from_labels_with_predictions_sqls(0.5)
    pipeline.enqueue_list_of_sqls(sqls)

    df_roc = linker._db_api.sql_pipeline_to_splink_dataframe(
        pipeline
    ).as_duckdbpyrelation()

    # Note that our critiera are great than or equal to
    # meaning match prob of 0.40 is treated as a match at threshold 0.40
    row = df_roc.filter("truth_threshold > 0.39 AND truth_threshold < 0.41")

    # FPR = FP/(FP+TN) = FP/N
    # At 0.4 we have
    # 1,11 is a TN
    # 2,12 is a P at 40% against a clerical N (45%) so is a FP
    # 3,13 is a FP as well

    assert pytest.approx(row.select("fp_rate").fetchone()[0]) == 2 / 3

    # Precision = TP/TP+FP
    assert row.select("precision").fetchone()[0] == 0.0


def test_roc_chart_dedupe_only(duckdb_with_fake_1000):
    # No source_dataset required in labels

    fake_1000 = duckdb_with_fake_1000.fake_1000
    con = duckdb_with_fake_1000.con
    df = fake_1000.filter("unique_id < 10")

    df_labels = df.query(
        "fake_1000_trimmed",
        """
        SELECT
            l.unique_id AS unique_id_l,
            r.unique_id AS unique_id_r,
            (l.cluster = r.cluster)::INT AS clerical_match_score
        FROM
            fake_1000_trimmed l
        JOIN
            fake_1000_trimmed r
        ON 1=1
        WHERE unique_id_l < unique_id_r
        """,
    )

    settings_dict = get_settings_dict()
    db_api = DuckDBAPI(connection=con)
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings_dict)

    labels_sdf = linker.table_management.register_table(df_labels, "labels")

    linker.evaluation.accuracy_analysis_from_labels_table(labels_sdf, output_type="roc")


def test_roc_chart_link_and_dedupe(duckdb_with_fake_1000):
    # Source dataset required in labels
    fake_1000 = duckdb_with_fake_1000.fake_1000
    con = duckdb_with_fake_1000.con
    df = fake_1000.filter("unique_id < 10").query(
        "fake_1000_trimmed",
        "SELECT 'fake_data_1' AS source_dataset, * FROM fake_1000_trimmed",
    )

    df_labels = df.query(
        "fake_1000_trimmed_with_source",
        """
        SELECT
            l.source_dataset AS source_dataset_l,
            r.source_dataset AS source_dataset_r,
            l.unique_id AS unique_id_l,
            r.unique_id AS unique_id_r,
            (l.cluster = r.cluster)::INT AS clerical_match_score
        FROM
            fake_1000_trimmed_with_source l
        JOIN
            fake_1000_trimmed_with_source r
        ON 1=1
        WHERE unique_id_l < unique_id_r
        """,
    )
    settings_dict = get_settings_dict()
    settings_dict["link_type"] = "link_and_dedupe"
    db_api = DuckDBAPI(connection=con)
    df_sdf = db_api.register(df, source_dataset_name="fake_data_1")

    linker = Linker(df_sdf, settings_dict)

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
    df = pa.Table.from_pylist(data)

    source_dataset_names = {
        "source_dataset_l": "fake_data_1",
        "source_dataset_r": "fake_data_1",
    }
    df_labels = pa.Table.from_pylist(
        [
            {
                "unique_id_l": 0,
                "unique_id_r": 1,
                "clerical_match_score": 0.8,
                **source_dataset_names,
            },
            {
                "unique_id_l": 1,
                "unique_id_r": 3,
                "clerical_match_score": 0.8,
                **source_dataset_names,
            },
            {
                "unique_id_l": 2,
                "unique_id_r": 3,
                "clerical_match_score": 0.8,
                **source_dataset_names,
            },
            {
                "unique_id_l": 4,
                "unique_id_r": 5,
                "clerical_match_score": 0.1,
                **source_dataset_names,
            },
        ]
    )

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
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table("labels")
    records = get_id_pairs_from_splink_dataframe(df_res)

    assert (1, 3) in records  # fn
    assert (2, 3) in records  # fn
    assert (4, 5) in records  # fp
    assert (1, 2) not in records  # tp

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table(
        "labels", include_false_negatives=False
    )
    records = get_id_pairs_from_splink_dataframe(df_res)

    assert (1, 3) not in records  # fn
    assert (2, 3) not in records  # fn
    assert (4, 5) in records  # fp
    assert (1, 2) not in records  # tp

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker.table_management.register_table(df_labels, "labels")

    pipeline = CTEPipeline()
    compute_df_concat_with_tf(linker, pipeline)

    df_res = linker.evaluation.prediction_errors_from_labels_table(
        "labels", include_false_positives=False
    )
    records = get_id_pairs_from_splink_dataframe(df_res)

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
    df = pa.Table.from_pylist(data)

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
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    df_res = linker.evaluation.prediction_errors_from_labels_column("cluster")
    records = get_id_pairs_from_splink_dataframe(df_res)

    assert (1, 3) in records  # FN
    assert (2, 3) in records  # FN
    assert (4, 5) in records  # FP
    assert (1, 2) not in records  # TP
    assert (1, 5) not in records  # TN

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    df_res = linker.evaluation.prediction_errors_from_labels_column(
        "cluster", include_false_positives=False
    )
    records = get_id_pairs_from_splink_dataframe(df_res)

    assert (1, 3) in records  # FN
    assert (2, 3) in records  # FN
    assert (4, 5) not in records  # FP
    assert (1, 2) not in records  # TP
    assert (1, 5) not in records  # TN

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    df_res = linker.evaluation.prediction_errors_from_labels_column(
        "cluster", include_false_negatives=False
    )
    records = get_id_pairs_from_splink_dataframe(df_res)

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

    df = pa.Table.from_pylist(data)

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
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

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

    df_left = pa.Table.from_pylist(data_left)
    df_right = pa.Table.from_pylist(data_right)

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
    df_left_sdf = db_api.register(df_left)
    df_right_sdf = db_api.register(df_right)

    linker = Linker([df_left_sdf, df_right_sdf], settings)

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


def compute_tp_tn_fp_fn(predictions_sdf: SplinkDataFrame, threshold: float):
    predictions_dict = predictions_sdf.as_dict()
    # Note that in Splink, our critiera are great than or equal to
    # meaning match prob of 0.40 is treated as a match at threshold 0.40
    predicted_matches = [mw >= threshold for mw in predictions_dict["match_weight"]]
    actual_matches = [
        cl == cr
        for cl, cr in zip(predictions_dict["cluster_l"], predictions_dict["cluster_r"])
    ]

    TP = sum(
        predicted_match and actual_match
        for predicted_match, actual_match in zip(predicted_matches, actual_matches)
    )
    TN = sum(
        not predicted_match and not actual_match
        for predicted_match, actual_match in zip(predicted_matches, actual_matches)
    )
    FP = sum(
        predicted_match and not actual_match
        for predicted_match, actual_match in zip(predicted_matches, actual_matches)
    )
    FN = sum(
        not predicted_match and actual_match
        for predicted_match, actual_match in zip(predicted_matches, actual_matches)
    )

    return TP, TN, FP, FN


def test_truth_space_table_from_column_vs_pandas_implementaiton_inc_unblocked(
    fake_1000,
):
    df = fake_1000.slice(length=50)

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

    db_api_pred = DuckDBAPI()
    df_sdf = db_api_pred.register(df)
    linker_for_predictions = Linker(df_sdf, settings)
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
    df_predictions = linker_for_predictions.misc.query_sql(sql, output_type="splink_df")

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

    db_api_answer = DuckDBAPI()
    df_sdf_answer = db_api_answer.register(df)
    linker_for_splink_answer = Linker(df_sdf_answer, settings)
    splink_rows = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_column(
            "cluster",
            output_type="table",
            positives_not_captured_by_blocking_rules_scored_as_zero=False,
        ).as_record_dict()
    )

    for splink_row in splink_rows:
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_column_vs_pandas_implementaiton_ex_unblocked(fake_1000):
    df = fake_1000.slice(length=100)

    df_1 = df.filter(pa.array([True, False] * 50))
    df_2 = df.filter(pa.array([False, True] * 50))

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

    db_api_pred = DuckDBAPI()
    df_1_sdf = db_api_pred.register(df_1)
    df_2_sdf = db_api_pred.register(df_2)
    linker_for_predictions = Linker([df_1_sdf, df_2_sdf], settings)
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
    df_predictions = linker_for_predictions.misc.query_sql(sql, output_type="splink_df")

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
    db_api_answer = DuckDBAPI()
    df_1_sdf_answer = db_api_answer.register(df_1)
    df_2_sdf_answer = db_api_answer.register(df_2)
    linker_for_splink_answer = Linker([df_1_sdf_answer, df_2_sdf_answer], settings)

    splink_rows = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_column(
            "cluster",
            output_type="table",
            positives_not_captured_by_blocking_rules_scored_as_zero=True,
        ).as_record_dict()
    )

    for splink_row in splink_rows:
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        # Make sure you print the values and ueful message if assert fails
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_table_vs_pandas_cartesian(fake_1000):
    df_first_50 = fake_1000.slice(length=50)
    sql = """
    SELECT
        l.unique_id as unique_id_l,
        r.unique_id as unique_id_r,
        case when l.cluster = r.cluster then 1 else 0 end as clerical_match_score
    from df_first_50 as l
    inner join df_first_50 as r
    on l.unique_id < r.unique_id
    """
    con = duckdb.connect()
    labels_table = con.sql(sql)

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

    db_api_pred = DuckDBAPI(con)
    df_first_50_sdf = db_api_pred.register(df_first_50)
    linker_for_predictions = Linker(df_first_50_sdf, settings)
    df_predictions = linker_for_predictions.inference.predict()

    db_api_answer = DuckDBAPI(con)
    df_sdf_answer = db_api_answer.register(fake_1000)
    linker_for_splink_answer = Linker(df_sdf_answer, settings)
    labels_input = linker_for_splink_answer.table_management.register_labels_table(
        labels_table
    )
    splink_rows = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_table(
            labels_input, output_type="table"
        ).as_record_dict()
    )

    for splink_row in splink_rows:
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"


def test_truth_space_table_from_table_vs_pandas_with_blocking(fake_1000):
    df_1 = fake_1000.filter([True, False] * 500)
    df_2 = fake_1000.filter([False, True] * 500)
    df_1 = df_1.append_column("source_dataset", pa.array(["left"] * df_1.num_rows))
    df_2 = df_2.append_column("source_dataset", pa.array(["right"] * df_2.num_rows))

    df_1_first_50 = df_1.slice(length=50)
    df_2_first_50 = df_2.slice(length=50)

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
    con = duckdb.connect()
    labels_table = con.sql(sql)

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

    db_api_pred = DuckDBAPI(con)
    df_1_first_50_sdf = db_api_pred.register(df_1_first_50)
    df_2_first_50_sdf = db_api_pred.register(df_2_first_50)
    linker_for_predictions = Linker([df_1_first_50_sdf, df_2_first_50_sdf], settings)
    df_predictions_raw = linker_for_predictions.inference.predict()
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
    df_predictions = linker_for_predictions.misc.query_sql(sql, output_type="splink_df")

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

    db_api_answer = DuckDBAPI(con)
    df_1_sdf_answer = db_api_answer.register(df_1)
    df_2_sdf_answer = db_api_answer.register(df_2)
    linker_for_splink_answer = Linker([df_1_sdf_answer, df_2_sdf_answer], settings)
    labels_input = linker_for_splink_answer.table_management.register_labels_table(
        labels_table
    )
    splink_rows = (
        linker_for_splink_answer.evaluation.accuracy_analysis_from_labels_table(
            labels_input, output_type="table"
        ).as_record_dict()
    )

    for splink_row in splink_rows:
        threshold = splink_row["truth_threshold"]
        # Compute stats using slow pandas methodology
        TP, TN, FP, FN = compute_tp_tn_fp_fn(df_predictions, threshold)
        assert TP == splink_row["tp"], f"TP: {TP} != {splink_row['tp']}"
        assert TN == splink_row["tn"], f"TN: {TN} != {splink_row['tn']}"
        assert FP == splink_row["fp"], f"FP: {FP} != {splink_row['fp']}"
        assert FN == splink_row["fn"], f"FN: {FN} != {splink_row['fn']}"

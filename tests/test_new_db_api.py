import os

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
from splink.exploratory import profile_columns
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding

comparison_name = cl.CustomComparison(
    output_column_name="name",
    comparison_levels=[
        cll.CustomLevel(
            "(first_name_l IS NULL OR first_name_r IS NULL) AND "
            "(surname_l IS NULL OR surname_r IS NULL) "
        ).configure(is_null_level=True),
        {
            "sql_condition": ("first_name_l || surname_l = first_name_r || surname_r"),
            "label_for_charts": "both names matching",
        },
        cll.CustomLevel(
            (
                "levenshtein("
                "first_name_l || surname_l, "
                "first_name_r || surname_r"
                ") <= 3"
            ),
            "both names fuzzy matching",
        ),
        cll.ExactMatchLevel("first_name"),
        cll.ExactMatchLevel("surname"),
        cll.ElseLevel(),
    ],
)
comparison_city = cl.ExactMatch("city").configure(u_probabilities=[0.6, 0.4])
comparison_email = cl.LevenshteinAtThresholds("email", 3).configure(
    m_probabilities=[0.8, 0.1, 0.1]
)
comparison_dob = cl.LevenshteinAtThresholds("dob", [1, 2])

cl_settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_name,
        comparison_city,
        comparison_email,
        comparison_dob,
    ],
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.first_name = r.first_name",
    ],
    "retain_intermediate_calculation_columns": True,
}


@mark_with_dialects_excluding()
def test_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = helper.DatabaseAPI(**helper.db_api_args())
    linker = Linker(
        df,
        cl_settings,
        db_api,
    )
    linker.inference.predict()


@mark_with_dialects_excluding()
def test_full_run(dialect, test_helpers, tmp_path):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = helper.DatabaseAPI(**helper.db_api_args())
    linker = Linker(
        df,
        cl_settings,
        db_api,
    )
    linker.training.estimate_probability_two_random_records_match(
        ["l.first_name = r.first_name AND l.surname = r.surname"],
        0.6,
    )
    linker.training.estimate_u_using_random_sampling(500)
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name"
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.surname = r.surname"
    )
    df_e = linker.inference.predict()
    df_c = linker.clustering.cluster_pairwise_predictions_at_threshold(df_e, 0.99)

    linker.visualisations.comparison_viewer_dashboard(
        df_e,
        os.path.join(tmp_path, "test_cvd_duckdb.html"),
        overwrite=True,
        num_example_rows=2,
    )
    linker.visualisations.cluster_studio_dashboard(
        df_e,
        df_c,
        os.path.join(tmp_path, "test_csd_duckdb.html"),
        overwrite=True,
    )


@mark_with_dialects_excluding()
def test_charts(dialect, test_helpers, tmp_path):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
        table_or_tables=df,
        blocking_rules=[block_on("dob"), block_on("first_name")],
        link_type="dedupe_only",
        db_api=db_api,
        unique_id_column_name="unique_id",
    )

    linker = Linker(df, cl_settings, db_api)

    linker.training.estimate_probability_two_random_records_match(
        ["l.first_name = r.first_name AND l.surname = r.surname"],
        0.6,
    )
    linker.training.estimate_u_using_random_sampling(500)
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name"
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.surname = r.surname"
    )

    linker.visualisations.match_weights_chart()
    linker.visualisations.m_u_parameters_chart()


@mark_with_dialects_excluding()
def test_exploratory_charts(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = helper.DatabaseAPI(**helper.db_api_args())
    profile_columns(df, db_api, "first_name")

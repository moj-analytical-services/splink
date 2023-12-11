import pandas as pd

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink.database_api import DuckDBAPI
from splink.linker import Linker

comparison_name = cl.CustomComparison(
    "name",
    [
        cll.CustomLevel(
            "(first_name_l IS NULL OR first_name_r IS NULL) AND "
            "(surname_l IS NULL OR surname_r IS NULL) "
        ).configure(is_null_level=True),
        {
            "sql_condition": (
                "concat(first_name_l, surname_l) = concat(first_name_r, surname_r)"
            ),
            "label_for_charts": "both names matching",
        },
        cll.CustomLevel(
            (
                "levenshtein("
                "concat(first_name_l, surname_l), "
                "concat(first_name_r, surname_r)"
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
}


def test_run_predict(test_helpers):
    # use dialect + helper to ease transition once we have all dialects back
    dialect = "duckdb"
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = DuckDBAPI()
    linker = Linker(
        df,
        cl_settings,
        db_api,
        # temporarily set this until we have dealt with it:
        accepted_df_dtypes=[pd.DataFrame],
    )
    linker.predict()


def test_full_run(test_helpers):
    # use dialect + helper to ease transition once we have all dialects back
    dialect = "duckdb"
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    db_api = DuckDBAPI()
    linker = Linker(
        df,
        cl_settings,
        db_api,
        # temporarily set this until we have dealt with it:
        accepted_df_dtypes=[pd.DataFrame],
    )
    linker.estimate_probability_two_random_records_match(
        ["l.first_name = r.first_name AND l.surname = r.surname"],
        0.6,
    )
    linker.estimate_u_using_random_sampling(500)
    linker.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name"
    )
    linker.estimate_parameters_using_expectation_maximisation("l.surname = r.surname")
    df_e = linker.predict()
    df_c = linker.cluster_pairwise_predictions_at_threshold(df_e, 0.99)

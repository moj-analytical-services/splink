# Regression test for https://github.com/moj-analytical-services/splink/issues/795

import os

from splink.duckdb.duckdb_linker import DuckDBLinker

from splink.duckdb.duckdb_comparison_level_library import _mutable_params
import pandas as pd


def test_regression(tmp_path):

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv").head(20)

    # Overwrite the surname comparison to include duck-db specific syntax

    _mutable_params["dialect"] = "duckdb"  # noqa: F811
    _mutable_params["levenshtein"] = "levenshtein"
    _mutable_params["jaro_winkler"] = "jaro_winkler_similarity"

    for rmc in [True, False]:
        for ricc in [True, False]:
            levels = [
                {
                    "sql_condition": '"first_name_l" IS NULL OR "first_name_r" IS NULL',
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": '"first_name_l" = "first_name_r"',
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "first_name",
                },
                {
                    "sql_condition": 'levenshtein("surname_l", "surname_r") <= 2',
                    "label_for_charts": "levenshtein <= 2",
                },
                {
                    "sql_condition": "ELSE",
                    "label_for_charts": "All other comparisons",
                },
            ]

            settings_dict = {
                "probability_two_random_records_match": 0.01,
                "link_type": "dedupe_only",
                "blocking_rules_to_generate_predictions": [
                    "l.first_name = r.first_name",
                    "l.surname = r.surname",
                ],
                "comparisons": [
                    {
                        "output_column_name": "first_name",
                        "comparison_levels": levels,
                        "comparison_description": "desc",
                    }
                ],
                "retain_matching_columns": rmc,
                "retain_intermediate_calculation_columns": ricc,
                "additional_columns_to_retain": ["group"],
                "max_iterations": 10,
                "em_convergence": 0.01,
            }

            linker = DuckDBLinker(
                df.copy(),
                settings_dict,
                connection=os.path.join(tmp_path, "duckdb.db"),
                output_schema="splink_in_duckdb",
            )

            linker.predict()

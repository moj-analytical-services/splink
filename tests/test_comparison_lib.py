import pandas as pd

import splink.duckdb.comparison_library as cl
from splink.duckdb.linker import DuckDBLinker


def test_distance_function_comparison():
    data = [
        {"unique_id": 1, "forename": "Harry", "surname": "Jones"},
        {"unique_id": 2, "forename": "Garry", "surname": "Johns"},
        {"unique_id": 3, "forename": "Barry", "surname": "James"},
        {"unique_id": 4, "forename": "Carry", "surname": "Jones"},
        {"unique_id": 5, "forename": "Cally", "surname": "Bones"},
        {"unique_id": 6, "forename": "Sally", "surname": "Jonas"},
    ]

    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.distance_function_at_thresholds(
                "forename", "hamming", [1, 2], higher_is_more_similar=False
            ),
            cl.distance_function_at_thresholds(
                "surname", "hamming", [1, 2], higher_is_more_similar=False
            ),
        ],
    }
    linker = DuckDBLinker(df, settings)

    df_pred = linker.predict().as_pandas_dataframe()

    expected_gamma_counts = {
        "forename": {
            # exact match
            3: 0,
            # Hamming 1 : 3 + 2 + 1 + 1
            2: 7,
            # Hamming 2 : 1
            1: 1,
            # Else
            0: 7,
        },
        "surname": {
            # exact match
            3: 1,
            # Hamming 1 : 2 + 2
            2: 4,
            # Hamming 2 : 2 + 2 + 1 + 1
            1: 6,
            # Else
            0: 4,
        },
    }

    for col, expected_counts in expected_gamma_counts.items():
        for gamma_val, expected_count in expected_counts.items():
            assert sum(df_pred[f"gamma_{col}"] == gamma_val) == expected_count


def test_set_to_lowercase_parameter():
    data = [
        {"id": 1, "forename": "John"},
        {"id": 2, "forename": "john"},
        {"id": 3, "forename": "Rob"},
        {"id": 4, "forename": "Rob"},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [cl.exact_match("forename", set_to_lowercase=True)],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)

    linker = DuckDBLinker(df, settings, validate_settings=False)
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])
    assert row["gamma_forename"] == 1

    row = dict(df_e.query("id_l == 3 and id_r == 4").iloc[0])
    assert row["gamma_forename"] == 1

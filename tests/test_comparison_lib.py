import pandas as pd

import splink.internals.comparison_library as cl
from splink.internals.column_expression import ColumnExpression
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import run_comparison_vector_value_tests


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
            cl.DistanceFunctionAtThresholds(
                "forename", "hamming", [1, 2], higher_is_more_similar=False
            ),
            cl.DistanceFunctionAtThresholds(
                "surname", "hamming", [1, 2], higher_is_more_similar=False
            ),
        ],
    }
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)

    df_pred = linker.inference.predict().as_pandas_dataframe()

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


@mark_with_dialects_excluding("sqlite", "spark", "postgres", "athena")
def test_pairwise_stringdistance_function_comparison(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    data = [
        {"unique_id": 1, "forename": ["Geof"]},
        {"unique_id": 2, "forename": ["Cally"]},
        {"unique_id": 3, "forename": ["Saly", "Barey"]},
        {"unique_id": 4, "forename": ["Geoff", "Barry"]},
        {"unique_id": 5, "forename": ["Carry"]},
        {"unique_id": 6, "forename": ["Cally", "Sally"]},
        {"unique_id": 7, "forename": ["Completely", "Different"]},
    ]

    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.PairwiseStringDistanceFunctionAtThresholds(
                "forename",
                "damerau_levenshtein",
                distance_threshold_or_thresholds=[1, 2],
            ),
        ],
    }

    linker = Linker(df, settings, db_api=db_api)

    df_pred = linker.inference.predict().as_pandas_dataframe()

    expected_gamma_counts = {
        "forename": {
            # exact match: Cally (1)
            3: 1,
            # damerau_levenshtein <= 1 :
            # Geof[f] (1) + Sal[l]y (1) + Bar[e/r]y (1) +
            # [B/C]arry (1)
            # -- note that [C/S]ally is not reached due to the exact match
            2: 4,
            # damerau_levenshtein <= 2 :
            # Ca[ll/rr]y (2) + [S/C]al[l]y (1) + [B/C]ar[e/r]y (1)
            1: 4,
            # Else: (7 * 6) / 2 - 1 - 4 - 4
            0: 12,
        },
    }

    for col, expected_counts in expected_gamma_counts.items():
        for gamma_val, expected_count in expected_counts.items():
            assert sum(df_pred[f"gamma_{col}"] == gamma_val) == expected_count


@mark_with_dialects_excluding()
def test_set_to_lowercase(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.ExactMatch(ColumnExpression("forename").lower()),
            "inputs": [
                {
                    "forename_l": "John",
                    "forename_r": "john",
                    "expected_value": 1,
                    "expected_label": "Exact match on transformed forename",
                },
                {
                    "forename_l": "Rob",
                    "forename_r": "Rob",
                    "expected_value": 1,
                    "expected_label": "Exact match on transformed forename",
                },
                {
                    "forename_l": "John",
                    "forename_r": "Jane",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)

import pandas as pd
from splink.duckdb.duckdb_linker import DuckDBLinker
import pytest


def test_prob_rr_match_dedupe():

    df = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "John", "surname": "Smith"},
            {"unique_id": 3, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 4, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 5, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 6, "first_name": "Jane", "surname": "Taylor"},
        ]
    )

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = DuckDBLinker(df, settings)
    linker.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 4 matches and 15 comparisons
    assert pytest.approx(prob) == 4 / 15

    # Test recall works
    deterministic_rules = ["l.first_name = r.first_name and l.surname = r.surname"]
    linker.estimate_probability_two_random_records_match(
        deterministic_rules, recall=0.9
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 4 matches and 15 comparisons
    assert pytest.approx(prob) == 4 / 15 * (1 / 0.9)


def test_prob_rr_match_link_only():

    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
            {"unique_id": 4, "first_name": "Alice", "surname": "Williams"},
        ]
    )

    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = DuckDBLinker([df_1, df_2], settings)
    linker.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 2 matches and 8 comparisons
    assert pytest.approx(prob) == 2 / 8


def test_prob_rr_match_link_and_dedupe():

    df_1 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smith"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 3, "first_name": "Jane", "surname": "Tailor"},
        ]
    )

    df_2 = pd.DataFrame(
        [
            {"unique_id": 1, "first_name": "John", "surname": "Smyth"},
            {"unique_id": 2, "first_name": "Mary", "surname": "Jones"},
            {"unique_id": 3, "first_name": "Jane", "surname": "Taylor"},
        ]
    )

    settings = {
        "link_type": "link_and_dedupe",
        "blocking_rules_to_generate_predictions": ["1=1"],
        "comparisons": [],
    }

    deterministic_rules = ["l.first_name = r.first_name", "l.surname = r.surname"]

    # Test dedupe only
    linker = DuckDBLinker([df_1, df_2], settings)
    linker.estimate_probability_two_random_records_match(
        deterministic_rules, recall=1.0
    )

    prob = linker._settings_obj._probability_two_random_records_match
    # 3 matches and 15 comparisons
    assert pytest.approx(prob) == 3 / 15

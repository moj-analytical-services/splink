import pandas as pd
import pytest

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import DuckDBAPI, SettingsCreator, block_on
from splink.internals.exceptions import EMTrainingException
from splink.internals.linker import Linker


def test_clear_error_when_empty_block():
    data = [
        {"unique_id": 1, "name": "Amanda", "surname": "Smith"},
        {"unique_id": 2, "name": "Robin", "surname": "Jones"},
        {"unique_id": 3, "name": "Robyn", "surname": "Williams"},
        {"unique_id": 4, "name": "David", "surname": "Green"},
        {"unique_id": 5, "name": "Eve", "surname": "Pope"},
        {"unique_id": 6, "name": "Amanda", "surname": "Anderson"},
    ]
    df = pd.DataFrame(data)

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.LevenshteinAtThresholds("name", 1),
            cl.ExactMatch("surname"),
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = True
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.training.estimate_parameters_using_expectation_maximisation(
        "l.name = r.name"
    )
    # No record pairs for which surname matches, so we should get a nice handled error
    with pytest.raises(EMTrainingException):
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.surname = r.surname"
        )


def test_estimate_without_term_frequencies():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("email"),
        ],
    }

    db_api = DuckDBAPI()

    linker_0 = Linker(df, settings, db_api=db_api)

    db_api = DuckDBAPI()

    linker_1 = Linker(df, settings, db_api=db_api)

    session_fast = linker_0.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule="l.email = r.email",
        estimate_without_term_frequencies=True,
    )
    session_slow = linker_1.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule="l.email = r.email",
        estimate_without_term_frequencies=False,
    )

    actual_prop_history = pd.DataFrame(session_fast._lambda_history_records)
    expected_prop_history = pd.DataFrame(session_slow._lambda_history_records)

    compare = expected_prop_history.merge(
        actual_prop_history,
        left_on="iteration",
        right_on="iteration",
        suffixes=["_e", "_a"],
    )

    for r in compare.to_dict(orient="records"):
        assert r["probability_two_random_records_match_e"] == pytest.approx(
            r["probability_two_random_records_match_a"]
        )

    actual_m_u_history = pd.DataFrame(session_fast._iteration_history_records)
    f1 = actual_m_u_history["comparison_name"] == "first_name"
    f2 = actual_m_u_history["comparison_vector_value"] == 1
    actual_first_name_level_1_m = actual_m_u_history[f1 & f2]

    expected_m_u_history = pd.DataFrame(session_slow._iteration_history_records)
    f1 = expected_m_u_history["comparison_name"] == "first_name"
    f2 = expected_m_u_history["comparison_vector_value"] == 1
    expected_first_name_level_1_m = expected_m_u_history[f1 & f2]

    compare = expected_first_name_level_1_m.merge(
        actual_first_name_level_1_m,
        left_on="iteration",
        right_on="iteration",
        suffixes=("_e", "_a"),
    )

    for r in compare.to_dict(orient="records"):
        assert r["m_probability_e"] == pytest.approx(r["m_probability_a"])


def test_fix_probabilities():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    first_name_comparison = cl.CustomComparison(
        comparison_levels=[
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("first_name").configure(
                m_probability=0.9999,
                fix_m_probability=True,
                u_probability=0.001,
                fix_u_probability=True,
            ),
            {
                "sql_condition": 'levenshtein("first_name_l", "first_name_r") <= 2',
                "label_for_charts": "Levenshtein distance of first_name <= 2",
                "m_probability": 0.88,
                "is_null_level": False,
                "fix_m_probability": True,
            },
            cll.ElseLevel().configure(
                m_probability=0.001,
                fix_m_probability=False,
                u_probability=0.9,
                fix_u_probability=False,
            ),
        ]
    )
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            first_name_comparison,
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("dob"),
        ],
        additional_columns_to_retain=["cluster"],
    )

    linker = Linker(df, settings, db_api=DuckDBAPI())

    linker.training.estimate_u_using_random_sampling(max_pairs=1e4)

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

    model = linker.misc.save_model_to_json()

    first_name_comparison = model["comparisons"][0]
    exact_match_level = first_name_comparison["comparison_levels"][1]
    levenshtein_level = first_name_comparison["comparison_levels"][2]

    assert (
        exact_match_level["m_probability"] == 0.9999
    ), "Exact match m_probability is not as expected"
    assert (
        exact_match_level["u_probability"] == 0.001
    ), "Exact match u_probability is not as expected"
    assert (
        levenshtein_level["m_probability"] == 0.88
    ), "Levenshtein m_probability is not as expected"

    # Check that non-fixed probabilities on the else level have changed
    else_level = first_name_comparison["comparison_levels"][3]

    assert (
        else_level["m_probability"] != 0.001
    ), "Else level m_probability should have changed"
    assert (
        else_level["u_probability"] != 0.9
    ), "Else level u_probability should have changed"

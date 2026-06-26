import pytest

import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import DuckDBAPI, SettingsCreator, block_on
from splink.internals.exceptions import EMTrainingException
from splink.internals.linker import Linker
from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_expectation_maximisation_runs(fake_1000, dialect, test_helpers):
    helper = test_helpers[dialect]
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city"),
        ],
    )
    db_api = helper.db_api()
    df_sdf = db_api.register(fake_1000)
    linker = Linker(df_sdf, settings)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name")
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname")
    )


def test_clear_error_when_empty_block():
    data = [
        {"unique_id": 1, "name": "Amanda", "surname": "Smith"},
        {"unique_id": 2, "name": "Robin", "surname": "Jones"},
        {"unique_id": 3, "name": "Robyn", "surname": "Williams"},
        {"unique_id": 4, "name": "David", "surname": "Green"},
        {"unique_id": 5, "name": "Eve", "surname": "Pope"},
        {"unique_id": 6, "name": "Amanda", "surname": "Anderson"},
    ]

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.LevenshteinAtThresholds("name", 1),
            cl.ExactMatch("surname"),
        ],
        "blocking_rules_to_generate_predictions": ["l.name = r.name"],
    }

    db_api = DuckDBAPI()
    df_sdf = db_api.register(data)

    linker = Linker(df_sdf, settings)
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


def test_estimate_without_term_frequencies(fake_1000):
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("email"),
        ],
    }

    db_api_1 = DuckDBAPI()
    df_sdf_1 = db_api_1.register(fake_1000)

    linker_0 = Linker(df_sdf_1, settings)

    db_api_2 = DuckDBAPI()
    df_sdf_2 = db_api_2.register(fake_1000)

    linker_1 = Linker(df_sdf_2, settings)

    session_fast = linker_0.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule="l.email = r.email",
        estimate_without_term_frequencies=True,
    )
    session_slow = linker_1.training.estimate_parameters_using_expectation_maximisation(
        blocking_rule="l.email = r.email",
        estimate_without_term_frequencies=False,
    )

    actual_prop_history = db_api_1.register(session_fast._lambda_history_records)
    expected_prop_history = db_api_1.register(session_slow._lambda_history_records)
    actuals = sorted(actual_prop_history.as_record_dict(), key=lambda r: r["iteration"])
    expecteds = sorted(
        expected_prop_history.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert expected["probability_two_random_records_match"] == pytest.approx(
            actual["probability_two_random_records_match"]
        )

    actual_m_u_history = db_api_2.register(
        list(map(lambda r: r.as_dict(), session_fast._iteration_history_records))
    )
    actual_first_name_level_1_m = actual_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE comparison_name = 'first_name'
            AND comparison_vector_value = 1
        """
    )

    expected_m_u_history = db_api_2.register(
        list(map(lambda r: r.as_dict(), session_slow._iteration_history_records))
    )
    expected_first_name_level_1_m = expected_m_u_history.query_sql(
        """
            SELECT *
            FROM {this}
            WHERE comparison_name = 'first_name'
            AND comparison_vector_value = 1
        """
    )

    actuals = sorted(
        actual_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )
    expecteds = sorted(
        expected_first_name_level_1_m.as_record_dict(), key=lambda r: r["iteration"]
    )

    for expected, actual in zip(expecteds, actuals):
        assert actual["m_probability"] == pytest.approx(expected["m_probability"])


def test_fix_probabilities(fake_1000):
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

    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

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


def test_fixed_match_weight_is_preserved_through_em_training(fake_1000):
    first_name_comparison = cl.CustomComparison(
        output_column_name="first_name",
        comparison_levels=[
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("first_name").configure(fixed_match_weight=3),
            cll.ElseLevel().configure(fixed_match_weight=-4),
        ],
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

    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)
    linker = Linker(df_sdf, settings)

    linker.training.estimate_u_using_random_sampling(max_pairs=1e4)
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

    # The in-memory model derives m/u from the fixed weight and never updates them
    first_name_levels = linker._settings_obj.comparisons[0].comparison_levels
    exact_obj = first_name_levels[1]
    assert exact_obj.fixed_match_weight == 3
    assert exact_obj.m_probability == 1.0
    assert exact_obj.u_probability == 2**-3

    else_obj = first_name_levels[2]
    assert else_obj.fixed_match_weight == -4
    assert else_obj.m_probability == 2**-4
    assert else_obj.u_probability == 1.0

    # The saved model serialises only the fixed match weight (no derived m/u)
    model = linker.misc.save_model_to_json()
    first_name_dicts = model["comparisons"][0]["comparison_levels"]

    exact_match_level = first_name_dicts[1]
    assert exact_match_level["fixed_match_weight"] == 3
    assert "m_probability" not in exact_match_level
    assert "u_probability" not in exact_match_level

    else_level = first_name_dicts[2]
    assert else_level["fixed_match_weight"] == -4
    assert "m_probability" not in else_level
    assert "u_probability" not in else_level

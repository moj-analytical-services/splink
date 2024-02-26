import pandas as pd

import splink.comparison_library as cl
from splink.database_api import DuckDBAPI
from splink.linker import Linker

# ground truth:
# true matches ALWAYS match on gender
# first_name is at most one edit away
# surname can be any match level
df = pd.DataFrame(
    [
        {
            "unique_id": 1,
            "true_match_id": 1,
            "first_name": "John",
            "surname": "Smith",
            "gender": "m",
            "tm_partial": 1,
        },
        {
            "unique_id": 2,
            "true_match_id": 1,
            "first_name": "Jon",
            "surname": "Smith",
            "gender": "m",
            "tm_partial": 1,
        },
        {
            "unique_id": 3,
            "true_match_id": 2,
            "first_name": "Mary",
            "surname": "Jones",
            "gender": "f",
            "tm_partial": None,
        },
        {
            "unique_id": 4,
            "true_match_id": 2,
            "first_name": "May",
            "surname": "Jones",
            "gender": "f",
            "tm_partial": 2,
        },
        {
            "unique_id": 5,
            "true_match_id": 2,
            "first_name": "Mary",
            "surname": "J",
            "gender": "f",
            "tm_partial": 2,
        },
        {
            "unique_id": 6,
            "true_match_id": 3,
            "first_name": "David",
            "surname": "Greene",
            "gender": "m",
            "tm_partial": 3,
        },
        {
            "unique_id": 7,
            "true_match_id": 3,
            "first_name": "David",
            "surname": "Green",
            "gender": "m",
            "tm_partial": None,
        },
        {
            "unique_id": 8,
            "true_match_id": 4,
            "first_name": "Sara",
            "surname": "Smith",
            "gender": "f",
            "tm_partial": 4,
        },
        {
            "unique_id": 9,
            "true_match_id": 4,
            "first_name": "Sarah",
            "surname": "Smith",
            "gender": "f",
            "tm_partial": 4,
        },
        {
            "unique_id": 10,
            "true_match_id": 4,
            "first_name": "Sarah",
            "surname": "Smt",
            "gender": "f",
            "tm_partial": 4,
        },
        {
            "unique_id": 11,
            "true_match_id": 5,
            "first_name": "Joan",
            "surname": "Smith",
            "gender": "f",
            "tm_partial": None,
        },
        {
            "unique_id": 12,
            "true_match_id": 6,
            "first_name": "Kim",
            "surname": "Greene",
            "gender": "f",
            "tm_partial": None,
        },
        {
            "unique_id": 13,
            "true_match_id": 7,
            "first_name": "Kim",
            "surname": "Greene",
            "gender": "m",
            "tm_partial": 7,
        },
    ]
)


def test_m_u_charts():
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("gender"),
            cl.ExactMatch("tm_partial"),
            cl.LevenshteinAtThresholds("surname", [1]),
        ],
    }
    db_api = DuckDBAPI()

    linker = Linker(df, settings, database_api=db_api)

    linker.estimate_probability_two_random_records_match(
        "l.true_match_id = r.true_match_id", recall=1.0
    )

    linker.estimate_parameters_using_expectation_maximisation(
        "l.surname = r.surname",
        fix_u_probabilities=False,
        fix_probability_two_random_records_match=True,
    )

    assert linker._settings_obj.comparisons[1].comparison_levels[1].u_probability == 0.0

    linker.match_weights_chart()


def test_parameter_estimate_charts():
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.ExactMatch("gender"),
            cl.LevenshteinAtThresholds("first_name", [1]),
            cl.LevenshteinAtThresholds("surname", [1]),
        ],
    }
    db_api = DuckDBAPI()

    linker = Linker(df, settings, database_api=db_api)

    linker.estimate_probability_two_random_records_match(
        "l.true_match_id = r.true_match_id", recall=1.0
    )

    linker.estimate_parameters_using_expectation_maximisation(
        "l.surname = r.surname",
        fix_u_probabilities=False,
        fix_probability_two_random_records_match=True,
    )
    linker.estimate_parameters_using_expectation_maximisation(
        "l.first_name = r.first_name",
        fix_u_probabilities=False,
        fix_probability_two_random_records_match=True,
    )

    exact_gender_m_estimates = [
        prob["probability"]
        for prob in linker._settings_obj.comparisons[0]
        .comparison_levels[1]
        ._trained_m_probabilities
    ]
    assert 1.0 in exact_gender_m_estimates

    linker.parameter_estimate_comparisons_chart()

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            # no observations of levenshtein == 1 in this data
            cl.LevenshteinAtThresholds("gender", [1]),
            cl.LevenshteinAtThresholds("first_name", [1]),
        ],
    }
    db_api = DuckDBAPI()

    linker = Linker(df, settings, database_api=db_api)
    linker.estimate_u_using_random_sampling(1e6)

    linker.parameter_estimate_comparisons_chart()

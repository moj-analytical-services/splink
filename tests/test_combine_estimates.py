import logging
from statistics import median

import pytest

from splink.combine_models import ModelCombiner
from splink.model import Model, load_model_from_json
from splink.settings import Settings

logger = logging.getLevelName


def test_api():
    model_1 = load_model_from_json("tests/params/params_1.json")
    model_2 = load_model_from_json("tests/params/params_2.json")
    model_3 = load_model_from_json("tests/params/params_3.json")
    mc = ModelCombiner(
        [model_1, model_2, model_3],
        ["first name block", "surname block", "dob block"],
    )
    mc.get_combined_settings_dict()
    mc.summary_report(aggregate_function=lambda x: sum(x) / len(x))
    mc.comparison_chart()


def test_calc(spark):
    settings_1 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.forename = r.forename"],
        "comparison_columns": [
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.9, 0.1],
            },
        ],
    }

    settings_2 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.surname = r.surname"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.9, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.85, 0.15],
            },
        ],
    }

    settings_3 = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.dob = r.dob"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.2, 0.4, 0.4],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "email",
                "num_levels": 2,
                "m_probabilities": [0.1, 0.9],
                "u_probabilities": [0.7, 0.3],
            },
        ],
    }

    p1 = Model(settings_1, spark)
    p2 = Model(settings_2, spark)
    p3 = Model(settings_3, spark)

    mc = ModelCombiner([p1, p2, p3], ["forename block", "surname block", "dob block"])

    settings_dict = mc.get_combined_settings_dict()

    settings = Settings(settings_dict)
    email = settings.get_comparison_column("email")
    actual = email["u_probabilities"][0]
    expected = median([0.9, 0.85, 0.7])
    assert actual == pytest.approx(expected)

    surname = settings.get_comparison_column("surname")
    actual = surname["m_probabilities"][2]
    expected = median([0.4, 0.5])
    assert actual == pytest.approx(expected)

    assert len(settings_dict["blocking_rules"]) == 3

    settings_4_with_nulls = {
        "link_type": "link_and_dedupe",
        "blocking_rules": ["l.email = r.email"],
        "comparison_columns": [
            {
                "col_name": "forename",
                "num_levels": 3,
                "m_probabilities": [None, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "m_probabilities": [0.1, 0.4, 0.5],
                "u_probabilities": [0.8, 0.1, 0.1],
            },
        ],
    }

    p4 = Model(settings_4_with_nulls, spark)
    mc = ModelCombiner(
        [p1, p2, p3, p4],
        ["forename block", "surname block", "dob block", "email_block"],
    )

    with pytest.warns(UserWarning):
        settings_dict = mc.get_combined_settings_dict()

    settings = Settings(settings_dict)
    forename = settings.get_comparison_column("forename")
    actual = forename["m_probabilities"][0]
    assert actual is None

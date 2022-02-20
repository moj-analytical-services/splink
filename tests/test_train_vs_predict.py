import pytest
from splink.comparison import Comparison
from splink.misc import bayes_factor_to_prob, prob_to_bayes_factor
from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker


import pandas as pd

first_name_cc = {
    "column_name": "first_name",
    "comparison_levels": [
        {
            "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "first_name_l = first_name_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.7,
            "u_probability": 0.1,
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 0.6,
        },
        {
            "sql_condition": "levenshtein(first_name_l, first_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 2",
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.8,
        },
    ],
}


surname_cc = {
    "column_name": "surname",
    "comparison_levels": [
        {
            "sql_condition": "surname_l IS NULL OR surname_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "surname_l = surname_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

dob_cc = {
    "column_name": "dob",
    "comparison_levels": [
        {
            "sql_condition": "dob_l IS NULL OR dob_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "dob_l = dob_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

email_cc = {
    "column_name": "email",
    "comparison_levels": [
        {
            "sql_condition": "email_l IS NULL OR email_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "email_l = email_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

city_cc = {
    "column_name": "city",
    "comparison_levels": [
        {
            "sql_condition": "city_l IS NULL OR city_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "city_l = city_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

bf_for_first_name = 0.9 / 0.1
glo = bayes_factor_to_prob(prob_to_bayes_factor(0.3) / bf_for_first_name)

settings_dict = {
    "proportion_of_matches": glo,
    "link_type": "link_and_dedupe",
    "blocking_rules_to_generate_predictions": [
        "l.surname = r.surname",
    ],
    "comparisons": [
        first_name_cc,
        surname_cc,
        dob_cc,
        email_cc,
        city_cc,
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "additional_columns_to_retain": ["group"],
    "em_convergence": 0.001,
    "max_iterations": 20,
}


def test_train_vs_predict():
    """
    If you train parameters blocking on a column (say first_name)
    and then predict() using blocking_rules_to_generate_predictions
    on first_name too, you should get the same answer.
    This is despite the fact proportion_of_matches differs
    in that it's global using predict() and local in train().

    The global version has the param estimate of first_name 'reveresed out'
    """

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings_dict["blocking_rules_to_generate_predictions"] = ["l.surname = r.surname"]
    linker = DuckDBInMemoryLinker(settings_dict, input_tables={"fake_data_1": df})

    training_session = linker.train_m_and_u_using_expectation_maximisation(
        "l.surname = r.surname"
    )

    expected = training_session.settings_obj._proportion_of_matches

    # We expect the proportion of matches to be the same as for a predict
    df = linker.predict()
    actual = df["match_probability"].mean()

    # Will not be exactly equal because expected represents the proportion of matches
    # in the final iteration of training, before m and u were updated for the final time
    # Set em_comvergence to be very tiny and max iterations very high to get them arbitrarily close

    assert expected == pytest.approx(actual, abs=0.01)
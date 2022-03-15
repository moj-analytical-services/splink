import pytest

from splink.duckdb.duckdb_linker import DuckDBLinker


import pandas as pd

from basic_settings import settings_dict


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
    linker = DuckDBLinker(settings_dict, input_tables={"fake_data_1": df})

    training_session = linker.train_m_and_u_using_expectation_maximisation(
        "l.surname = r.surname"
    )

    expected = training_session.settings_obj._proportion_of_matches

    # We expect the proportion of matches to be the same as for a predict
    df = linker.predict().as_pandas_dataframe()
    actual = df["match_probability"].mean()

    # Will not be exactly equal because expected represents the proportion of matches
    # in the final iteration of training, before m and u were updated for the final time
    # Set em_comvergence to be very tiny and max iterations very high to get them
    # arbitrarily close

    assert expected == pytest.approx(actual, abs=0.01)

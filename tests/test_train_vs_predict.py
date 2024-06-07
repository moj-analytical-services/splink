import pytest

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_train_vs_predict(test_helpers, dialect):
    """
    If you train parameters blocking on a column (say first_name)
    and then predict() using blocking_rules_to_generate_predictions
    on first_name too, you should get the same answer.
    This is despite the fact probability_two_random_records_match differs
    in that it's global using predict() and local in train().

    The global version has the param estimate of first_name 'reveresed out'
    """
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()
    settings_dict["blocking_rules_to_generate_predictions"] = ["l.surname = r.surname"]
    linker = helper.Linker(df, settings_dict, **helper.extra_linker_args())

    training_session = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            "l.surname = r.surname", fix_u_probabilities=False
        )
    )

    expected = training_session.core_model_settings.probability_two_random_records_match

    # We expect the probability_two_random_records_match to be the same as for a predict
    df = linker.inference.predict().as_pandas_dataframe()
    actual = df["match_probability"].mean()

    # Will not be exactly equal because expected represents the
    # probability_two_random_records_match
    # in the final iteration of training, before m and u were updated for the final time
    # Set em_comvergence to be very tiny and max iterations very high to get them
    # arbitrarily close

    assert expected == pytest.approx(actual, abs=0.01)

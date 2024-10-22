import os
from functools import wraps

import pandas as pd

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.exploratory import profile_columns

from .decorator import mark_with_dialects_excluding


class DebugModeError(Exception): ...


df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("dob"),
        block_on("first_name", "surname"),
    ],
    retain_intermediate_calculation_columns=True,
)


def debug_mode_test_wrapper(test_function):
    @wraps(test_function)
    def wrapper(*args, **kwargs):
        errors = {}
        for debug_mode in (False, True):
            kwargs["debug_mode"] = debug_mode
            try:
                test_function(*args, **kwargs)
            except Exception as e:
                errors[debug_mode] = e
        if errors:
            # in this case we get an error in normal test execution
            # raise, but this is not a debug_mode issue
            if errors.get(False, False):
                raise errors[False]
            # this is a debug mode error only!
            raise DebugModeError(
                f"Error occurs in debug mode only: {errors[True]}"
            ) from errors[True]

    return wrapper


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_u_training(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_ptrrm_train(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_em_training(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_combined_training(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_predict(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.inference.predict(0.8)


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_clustering(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    df_e = linker.inference.predict(0.8)
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_e, 0.9)


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_match_weights_chart(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.visualisations.match_weights_chart()


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_m_u_chart(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.visualisations.m_u_parameters_chart()


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_tf_adjustments_chart(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.visualisations.tf_adjustment_chart("city")


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_interactive_match_weights_chart(
    test_helpers, dialect, debug_mode=None
):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("dob")
    )
    em_session.match_weights_interactive_history_chart()


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_interactive_m_u_chart(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    em_session = linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("dob")
    )
    em_session.m_u_values_interactive_history_chart()


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_unlinkables_chart(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.evaluation.unlinkables_chart()


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_profile_columns(test_helpers, dialect, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    db_api.debug_mode = debug_mode

    profile_columns(df, db_api)


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_comparison_viewer(test_helpers, dialect, tmp_path, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    df_e = linker.inference.predict(0.8)

    linker.visualisations.comparison_viewer_dashboard(
        df_e, os.path.join(tmp_path, "test_cvd.html"), overwrite=True
    )


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_cluster_studio(test_helpers, dialect, tmp_path, debug_mode=None):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    df_e = linker.inference.predict(0.8)
    df_c = linker.clustering.cluster_pairwise_predictions_at_threshold(df_e, 0.9)

    linker.visualisations.cluster_studio_dashboard(
        df_e, df_c, os.path.join(tmp_path, "test_csd.html"), overwrite=True
    )

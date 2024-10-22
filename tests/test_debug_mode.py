import pandas as pd

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding


class DebugModeError(Exception): ...


df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("dob"),
        block_on("first_name", "surname"),
    ],
)


def debug_mode_test_wrapper(test_function):
    def wrapper(test_helpers, dialect):
        errors = {}
        for debug_mode in (False, True):
            try:
                test_function(test_helpers, dialect, debug_mode=debug_mode)
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
def test_debug_mode_u_training(test_helpers, dialect, debug_mode):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)


@mark_with_dialects_excluding()
@debug_mode_test_wrapper
def test_debug_mode_ptrrm_train(test_helpers, dialect, debug_mode):
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
def test_debug_mode_combined_training(test_helpers, dialect, debug_mode):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)

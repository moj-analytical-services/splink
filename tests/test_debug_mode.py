import pandas as pd
from pytest import mark, param

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding

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


@mark.parametrize(
    "debug_mode", [param(False, id="debug_mode off"), param(True, id="debug_mode on")]
)
@mark_with_dialects_excluding()
def test_debug_mode_u_training(test_helpers, dialect, debug_mode):
    helper = test_helpers[dialect]

    db_api = helper.DatabaseAPI(**helper.db_api_args())

    linker = Linker(df, settings, db_api)
    db_api.debug_mode = debug_mode

    linker.training.estimate_u_using_random_sampling(max_pairs=6e5)

@mark.parametrize(
    "debug_mode", [param(False, id="debug_mode off"), param(True, id="debug_mode on")]
)
@mark_with_dialects_excluding()
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

@mark.parametrize(
    "debug_mode", [param(False, id="debug_mode off"), param(True, id="debug_mode on")]
)
@mark_with_dialects_excluding()
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

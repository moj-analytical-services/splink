import splink.internals.comparison_library as cl
from splink import SettingsCreator, block_on
from splink.internals.datasets import splink_datasets


def test_datasets_basic_link(test_helpers):
    # use duckdb as a backend just to check data is roughly as expected
    # don't need to check other backends as that's not what we're testing
    helper = test_helpers["duckdb"]

    df = splink_datasets.fake_1000
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
    )
    linker = helper.Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    linker.inference.predict()

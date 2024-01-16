import splink.comparison_library as cl
from splink.datasets import splink_datasets


def test_datasets_basic_link(test_helpers):
    # use duckdb as a backend just to check data is roughly as expected
    # don't need to check other backends as that's not what we're testing
    helper = test_helpers["duckdb"]

    df = splink_datasets.fake_1000
    linker = helper.Linker(
        df,
        {
            "link_type": "dedupe_only",
            "comparisons": [cl.ExactMatch("first_name"), cl.ExactMatch("surname")],
        },
        **helper.extra_linker_args()
    )
    linker.predict()

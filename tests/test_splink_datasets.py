from splink.datasets import splink_datasets


def test_datasets_basic_link(test_helpers):
    # use duckdb as a backend just to check data is roughly as expected
    # don't need to check other backends as that's not what we're testing
    helper = test_helpers["duckdb"]
    cl = helper.cl

    df = splink_datasets.fake_1000
    linker = helper.Linker(
        df,
        {
            "link_type": "dedupe_only",
            "comparisons": [cl.exact_match("first_name"), cl.exact_match("surname")],
        },
        **helper.extra_linker_args()
    )
    linker.predict()

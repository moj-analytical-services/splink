from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_missingness_chart(dialect, test_helpers):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, {"link_type": "dedupe_only"}, **helper.extra_linker_args())
    linker.missingness_chart()

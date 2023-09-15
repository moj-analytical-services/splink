import pandas as pd
from pytest import raises

from splink.exceptions import SplinkException
from tests.decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_missingness_chart(dialect, test_helpers):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(
        df, {"link_type": "dedupe_only"}, **helper.extra_linker_args()
    )
    linker.missingness_chart()


@mark_with_dialects_excluding()
def test_missingness_chart_mismatched_columns(dialect, test_helpers):
    helper = test_helpers[dialect]

    df_l = helper.load_frame_from_csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv"
    )
    df_r = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_r.rename(columns={"surname": "SURNAME"}, inplace=True)
    df_r = helper.convert_frame(df_r)

    linker = helper.Linker(
        [df_l, df_r], {"link_type": "link_only"}, **helper.extra_linker_args()
    )
    with raises(SplinkException):
        linker.missingness_chart()

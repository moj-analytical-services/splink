import pandas as pd
from pytest import raises

from splink.internals.exceptions import SplinkException
from splink.exploratory import completeness_chart
from tests.decorator import mark_with_dialects_excluding


# The UNION ALL used for this chart gives a
# "(": syntax error in sqlite
@mark_with_dialects_excluding("sqlite")
def test_completeness_chart(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    completeness_chart(df, db_api)
    completeness_chart(df, db_api, cols=["first_name", "surname"])
    completeness_chart(df, db_api, cols=["first_name"], table_names_for_chart=["t1"])


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_mismatched_columns(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    df_l = helper.load_frame_from_csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv"
    )
    df_r = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_r.rename(columns={"surname": "surname_2"}, inplace=True)
    df_r = helper.convert_frame(df_r)

    with raises(SplinkException):
        completeness_chart([df_l, df_r], db_api)

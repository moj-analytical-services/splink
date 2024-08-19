import pandas as pd
from pytest import raises

from splink.exploratory import completeness_chart
from splink.internals.exceptions import SplinkException
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


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_complex_columns(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "first_name": ["Arnie", None, "Carla", "Debbie", None],
            "surname": [None, None, None, None, "Everett"],
            "city_arr": [
                ["London", "Leeds"],
                ["Birmingham"],
                None,
                ["Leeds", "Manchester"],
                None,
            ],
        }
    )
    df = helper.convert_frame(df)
    first = helper.arrays_from
    # check completeness when we have more complicated column constructs, such as
    # indexing into array columns
    completeness_chart(df, db_api, cols=["first_name", "surname", f"city_arr[{first}]"])


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_source_dataset(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_pd["source_dataset"] = "fake_1000"
    df = helper.convert_frame(df_pd)
    completeness_chart(df, db_api)

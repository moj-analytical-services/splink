import pyarrow as pa
from pytest import raises

from splink.exploratory import completeness_chart
from splink.internals.exceptions import SplinkException
from tests.decorator import mark_with_dialects_excluding
from tests.helpers import TestHelper


# The UNION ALL used for this chart gives a
# "(": syntax error in sqlite
@mark_with_dialects_excluding("sqlite")
def test_completeness_chart(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.db_api()
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df_sdf = db_api.register(df)
    completeness_chart(df_sdf)
    completeness_chart(df_sdf, cols=["first_name", "surname"])
    completeness_chart(df_sdf, cols=["first_name"], table_names_for_chart=["t1"])


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_mismatched_columns(dialect, test_helpers, fake_1000):
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    df_l_sdf = db_api.register(fake_1000)
    df_r_sdf = db_api.register(fake_1000.rename_columns(names={"surname": "surname_2"}))

    with raises(SplinkException):
        completeness_chart([df_l_sdf, df_r_sdf])


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_complex_columns(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.db_api()
    data = {
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

    df_sdf = db_api.register(data)
    first = helper.arrays_from
    # check completeness when we have more complicated column constructs, such as
    # indexing into array columns
    completeness_chart(df_sdf, cols=["first_name", "surname", f"city_arr[{first}]"])


@mark_with_dialects_excluding("sqlite")
def test_completeness_chart_source_dataset(
    dialect: str, test_helpers: dict[str, TestHelper], fake_1000: pa.Table
):
    helper = test_helpers[dialect]
    db_api = helper.db_api()

    pa_df = fake_1000.append_column("source_dataset", pa.array(1000 * ["fake_1000"]))
    assert pa_df["source_dataset"]
    df_sdf = db_api.register(pa_df)
    completeness_chart(df_sdf)

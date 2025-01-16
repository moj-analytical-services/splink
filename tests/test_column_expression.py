import pandas as pd

from splink import ColumnExpression
from splink.internals.dialects import SplinkDialect

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite")
def test_access_extreme_array_element(test_helpers, dialect):
    # test data!
    df_arr = pd.DataFrame(
        [
            {"unique_id": 1, "name_arr": ["first", "middle", "last"]},
            {"unique_id": 2, "name_arr": ["aardvark", "llama", "ssnake", "zzebra"]},
            {"unique_id": 3, "name_arr": ["only"]},
        ]
    )

    helper = test_helpers[dialect]
    # register table with backend
    table_name = "arr_tab"
    table = helper.convert_frame(df_arr)
    db_api = helper.DatabaseAPI(**helper.db_api_args())
    arr_tab = db_api.register_table(table, table_name)

    # construct a SQL query from ColumnExpressions and run it against backend
    splink_dialect = SplinkDialect.from_string(dialect)
    first_element = ColumnExpression(
        "name_arr", sql_dialect=splink_dialect
    ).access_extreme_array_element("first")
    last_element = ColumnExpression(
        "name_arr", sql_dialect=splink_dialect
    ).access_extreme_array_element("last")
    sql = (
        f"SELECT unique_id, {first_element.name} AS first_element, "
        f"{last_element.name} AS last_element "
        f"FROM {arr_tab.physical_name} ORDER BY unique_id"
    )
    res = db_api.sql_to_splink_dataframe_checking_cache(
        sql, "test_first"
    ).as_pandas_dataframe()

    pd.testing.assert_series_equal(
        res["first_element"],
        pd.Series(["first", "aardvark", "only"], name="first_element"),
    )
    pd.testing.assert_series_equal(
        res["last_element"], pd.Series(["last", "zzebra", "only"], name="last_element")
    )


@mark_with_dialects_excluding()
def test_nullif(test_helpers, dialect):
    df_arr = pd.DataFrame(
        [
            {"unique_id": 1, "name": "name_1"},
            {"unique_id": 2, "name": ""},
            {"unique_id": 3, "name": None},
            {"unique_id": 4, "name": "name_4"},
            # dataset-specific invalid value:
            {"unique_id": 5, "name": "NA"},
        ]
    )

    helper = test_helpers[dialect]
    # register table with backend
    table_name = "nully_name_table"
    table = helper.convert_frame(df_arr)
    db_api = helper.DatabaseAPI(**helper.db_api_args())
    nully_table = db_api.register_table(table, table_name)

    # construct a SQL query from ColumnExpressions and run it against backend
    splink_dialect = SplinkDialect.from_string(dialect)
    nullif_name_empty_or_na = (
        ColumnExpression("name", sql_dialect=splink_dialect).nullif("").nullif("NA")
    )

    sql = (
        f"SELECT unique_id, {nullif_name_empty_or_na.name} AS cleaned_name "
        f"FROM {nully_table.physical_name} ORDER BY unique_id"
    )
    res = db_api.sql_to_splink_dataframe_checking_cache(
        sql, "test_first"
    ).as_pandas_dataframe()

    pd.testing.assert_series_equal(
        res["cleaned_name"],
        pd.Series(["name_1", None, None, "name_4", None], name="cleaned_name"),
    )

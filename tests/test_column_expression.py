from splink import ColumnExpression
from splink.internals.dialects import SplinkDialect

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding("sqlite")
def test_access_extreme_array_element(test_helpers, dialect):
    # test data!
    data_arr = [
        {"unique_id": 1, "name_arr": ["first", "middle", "last"]},
        {"unique_id": 2, "name_arr": ["aardvark", "llama", "ssnake", "zzebra"]},
        {"unique_id": 3, "name_arr": ["only"]},
    ]

    helper = test_helpers[dialect]
    # register table with backend
    db_api = helper.db_api()
    arr_tab = db_api.register(data_arr)

    # construct a SQL query from ColumnExpressions and run it against backend
    splink_dialect = SplinkDialect.from_string(dialect)
    first_element = ColumnExpression(
        "name_arr", sql_dialect=splink_dialect
    ).access_extreme_array_element("first")
    last_element = ColumnExpression(
        "name_arr", sql_dialect=splink_dialect
    ).access_extreme_array_element("last")
    sql = f"""
        SELECT
            unique_id,
            {first_element.name} AS first_element,
            {last_element.name} AS last_element
        FROM
            {{this}}
        ORDER BY
            unique_id
    """
    res = arr_tab.query_sql(sql).as_dict()

    assert res["first_element"] == ["first", "aardvark", "only"]
    assert res["last_element"] == ["last", "zzebra", "only"]


@mark_with_dialects_excluding()
def test_nullif(test_helpers, dialect):
    data_arr = [
        {"unique_id": 1, "name": "name_1"},
        {"unique_id": 2, "name": ""},
        {"unique_id": 3, "name": None},
        {"unique_id": 4, "name": "name_4"},
        # dataset-specific invalid value:
        {"unique_id": 5, "name": "NA"},
    ]

    helper = test_helpers[dialect]
    # register table with backend
    db_api = helper.db_api()
    nully_table = db_api.register(data_arr)

    # construct a SQL query from ColumnExpressions and run it against backend
    splink_dialect = SplinkDialect.from_string(dialect)
    # need to include nan as spark conerts np.nan to "nan", and in pandas 3
    # None gets converted to np.nan in string columns
    nullif_name_empty_or_na = (
        ColumnExpression("name", sql_dialect=splink_dialect)
        .nullif("")
        .nullif("NA")
        .nullif("nan")
    )

    sql = f"""
        SELECT
            unique_id,
            {nullif_name_empty_or_na.name} AS cleaned_name
        FROM
            {{this}}
        ORDER BY
            unique_id
    """
    res = nully_table.query_sql(sql).as_dict()

    assert res["cleaned_name"] == ["name_1", None, None, "name_4", None]

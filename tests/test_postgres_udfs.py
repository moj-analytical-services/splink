import pandas as pd
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import INTEGER

from splink.internals.postgres.database_api import PostgresAPI

from .decorator import mark_with_dialects_including


@mark_with_dialects_including("postgres")
def test_log2(pg_engine):
    db_api = PostgresAPI(engine=pg_engine)
    df = pd.DataFrame({"x": [2, 8, 0.5, 1]})
    expected_log2_vals = [1, 3, -1, 0]
    db_api.register_table(df, "log_values")
    sql = """SELECT log2("x") AS logs FROM log_values"""
    frame = db_api.sql_to_splink_dataframe_checking_cache(
        sql, "test_log_table"
    ).as_pandas_dataframe()

    for log_result, expected in zip(frame["logs"], expected_log2_vals):
        assert log_result == expected


@mark_with_dialects_including("postgres")
def test_array_intersect(pg_engine):
    db_api = PostgresAPI(engine=pg_engine)
    df = pd.DataFrame(
        [
            {"arr_l": [1, 2, 3], "arr_r": [1, 5, 6], "expected": [1]},
            {"arr_l": [1, 2, 3], "arr_r": [10, 1, -2], "expected": [1]},
            {"arr_l": [1, 2, 3], "arr_r": [4, 5, 6], "expected": []},
            {"arr_l": [1, 2, 3], "arr_r": [2, 1, 7, 10], "expected": [1, 2]},
            {"arr_l": [1, 2, 3], "arr_r": [1, 1, 1], "expected": [1]},
            {"arr_l": [1, 1, 1], "arr_r": [1, 2, 3], "expected": [1]},
            {"arr_l": [3, 5, 7], "arr_r": [3, 5, 7], "expected": [3, 5, 7]},
            {"arr_l": [1, 2, 3, 4, 5], "arr_r": [3, 5, 7], "expected": [3, 5]},
        ]
    )
    expected_intersect_vals = df["expected"]
    df.to_sql(
        "intersect_vals",
        pg_engine,
        dtype={"arr_l": postgresql.ARRAY(INTEGER), "arr_r": postgresql.ARRAY(INTEGER)},
    )
    sql = "SELECT array_intersect(arr_l, arr_r) AS intersects FROM intersect_vals"
    frame = db_api.sql_to_splink_dataframe_checking_cache(
        sql, "test_intersect_table"
    ).as_pandas_dataframe()

    for int_result, expected in zip(frame["intersects"], expected_intersect_vals):
        # don't care about order
        assert set(int_result) == set(expected)
        # should check we don't have duplicates
        assert len(int_result) == len(expected)

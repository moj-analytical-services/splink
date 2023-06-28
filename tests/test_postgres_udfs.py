import pandas as pd
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import INTEGER

from splink.postgres.linker import PostgresLinker

from .decorator import mark_with_dialects_including


@mark_with_dialects_including("postgres")
def test_log2(pg_engine):
    linker = PostgresLinker(
        [],
        engine=pg_engine,
    )
    df = pd.DataFrame({"x": [2, 8, 0.5, 1]})
    expected_log2_vals = [1, 3, -1, 0]
    linker.register_table(df, "log_values")
    sql = """SELECT log2("x") AS logs FROM log_values"""
    frame = linker._execute_sql_against_backend(
        sql, "dummy_name", "test_log_table"
    ).as_pandas_dataframe()

    for log_result, expected in zip(frame["logs"], expected_log2_vals):
        assert log_result == expected


@mark_with_dialects_including("postgres")
def test_datediff(pg_engine):
    linker = PostgresLinker(
        [],
        engine=pg_engine,
    )
    df = pd.DataFrame(
        [
            {"date_l": "2023-05-23", "date_r": "2023-05-24", "expected": -1},
            {"date_l": "2023-05-24", "date_r": "2023-05-23", "expected": 1},
            {"date_l": "2023-05-23", "date_r": "2022-05-23", "expected": 365},
            # 365*3 + 366
            {"date_l": "2023-07-22", "date_r": "2019-07-22", "expected": 1461},
        ]
    )
    fmt = "YYYY-MM-DD"
    expected_datediff_vals = df["expected"]
    linker.register_table(df, "datediff_vals")
    sql = f"""
    SELECT datediff(
        to_date("date_l", '{fmt}'), to_date("date_r", '{fmt}')
    ) AS datediffs FROM datediff_vals"""
    frame = linker._execute_sql_against_backend(
        sql, "dummy_name", "test_dd_table"
    ).as_pandas_dataframe()

    for dd_result, expected in zip(frame["datediffs"], expected_datediff_vals):
        assert dd_result == expected


@mark_with_dialects_including("postgres")
def test_months_between(pg_engine):
    # NB only testing floor of this function, as that is what we have in datediff
    linker = PostgresLinker(
        [],
        engine=pg_engine,
    )
    df = pd.DataFrame(
        [
            {"date_l": "2023-05-24", "date_r": "2023-05-23", "expected": 0},
            {"date_l": "2023-05-24", "date_r": "2023-04-25", "expected": 0},
            {"date_l": "2023-05-24", "date_r": "2023-02-25", "expected": 2},
            {"date_l": "2023-05-24", "date_r": "2022-05-23", "expected": 12},
            {"date_l": "1995-09-30", "date_r": "1996-03-25", "expected": -6},
        ]
    )
    fmt = "YYYY-MM-DD"
    expected_monthdiff_vals = df["expected"]
    linker.register_table(df, "monthdiff_vals")
    sql = f"""
    SELECT floor(
        ave_months_between(
            to_date("date_l", '{fmt}'), to_date("date_r", '{fmt}')
        )
    ) AS monthdiffs FROM monthdiff_vals"""
    frame = linker._execute_sql_against_backend(
        sql, "dummy_name", "test_md_table"
    ).as_pandas_dataframe()

    for md_result, expected in zip(frame["monthdiffs"], expected_monthdiff_vals):
        assert md_result == expected


@mark_with_dialects_including("postgres")
def test_array_intersect(pg_engine):
    linker = PostgresLinker(
        [],
        engine=pg_engine,
    )
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
    frame = linker._execute_sql_against_backend(
        sql, "dummy_name", "test_intersect_table"
    ).as_pandas_dataframe()

    for int_result, expected in zip(frame["intersects"], expected_intersect_vals):
        # don't care about order
        assert set(int_result) == set(expected)
        # should check we don't have duplicates
        assert len(int_result) == len(expected)

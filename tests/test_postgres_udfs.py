import pandas as pd

from splink.postgres.postgres_linker import PostgresLinker


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


def test_datediff(pg_engine):
    linker = PostgresLinker(
        [],
        engine=pg_engine,
    )
    df = pd.DataFrame(
        [
            {
                "date_l": "2023-05-23", "date_r": "2023-05-24", "expected": -1
            },
            {
                "date_l": "2023-05-24", "date_r": "2023-05-23", "expected": 1
            },
            {
                "date_l": "2023-05-23", "date_r": "2022-05-23", "expected": 365
            },
            # 365*3 + 366
            {
                "date_l": "2023-07-22", "date_r": "2019-07-22", "expected": 1461
            },
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
    
    for log_result, expected in zip(frame["datediffs"], expected_datediff_vals):
        assert log_result == expected

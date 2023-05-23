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

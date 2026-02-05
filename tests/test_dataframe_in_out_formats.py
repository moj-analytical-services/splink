from pytest import mark

from tests.decorator import mark_with_dialects_excluding

input_data_list = [
    {"id": 1, "backend": "duckdb"},
    {"id": 2, "backend": "spark"},
    {"id": 3, "backend": "postgres"},
]
input_data_dict = {
    "id": [1, 2, 3],
    "backend": ["duckdb", "spark", "postgres"],
}

table_sql = """
SELECT 1 as id, 'duckdb' as backend
UNION ALL
SELECT 2 as id, 'spark' as backend
UNION ALL
SELECT 3 as id, 'postgres' as backend
"""


@mark_with_dialects_excluding()
def test_splink_dataframe_from_list(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()
    db_api.register(input_data_list, unique_per_test_table_name)
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_from_tuple(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()
    db_api.register(tuple(input_data_list), unique_per_test_table_name)
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_from_dict(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()
    db_api.register(input_data_dict, unique_per_test_table_name)
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
@mark.needs_pandas
def test_splink_dataframe_from_pandas(
    dialect, test_helpers, unique_per_test_table_name
):
    import pandas as pd

    helper = test_helpers[dialect]

    db_api = helper.db_api()
    db_api.register(pd.DataFrame(input_data_dict), unique_per_test_table_name)
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_from_pyarrow(
    dialect, test_helpers, unique_per_test_table_name
):
    import pyarrow as pa

    helper = test_helpers[dialect]

    db_api = helper.db_api()
    db_api.register(pa.Table.from_pydict(input_data_dict), unique_per_test_table_name)
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
@mark.needs_pandas
def test_splink_dataframe_to_pandas(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    sdf = db_api._sql_to_splink_dataframe(
        table_sql, "test_table", unique_per_test_table_name
    )
    sdf.as_pandas_dataframe()
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_to_duckdb(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    sdf = db_api._sql_to_splink_dataframe(
        table_sql, "test_table", unique_per_test_table_name
    )
    sdf.as_duckdbpyrelation()
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_to_pyarrow(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    sdf = db_api._sql_to_splink_dataframe(
        table_sql, "test_table", unique_per_test_table_name
    )
    sdf.as_pyarrow_table()
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_to_list(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    sdf = db_api._sql_to_splink_dataframe(
        table_sql, "test_table", unique_per_test_table_name
    )
    sdf.as_record_dict()
    db_api.delete_table_from_database(unique_per_test_table_name)


@mark_with_dialects_excluding()
def test_splink_dataframe_to_dict(dialect, test_helpers, unique_per_test_table_name):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    sdf = db_api._sql_to_splink_dataframe(
        table_sql, "test_table", unique_per_test_table_name
    )
    sdf.as_dict()
    db_api.delete_table_from_database(unique_per_test_table_name)

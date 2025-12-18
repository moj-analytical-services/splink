import pyarrow as pa
from pytest import mark

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_register_list_data(dialect, test_helpers):
    helper = test_helpers[dialect]

    test_data_list = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    table_name = "test_table"
    db_api.register_table(
        input_table=test_data_list,
        table_name=table_name,
    )
    db_api.delete_table_from_database(table_name)


@mark_with_dialects_excluding()
def test_register_dict_data(dialect, test_helpers):
    helper = test_helpers[dialect]

    test_data_dict = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    }
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    table_name = "test_table"
    db_api.register_table(
        input_table=test_data_dict,
        table_name=table_name,
    )
    db_api.delete_table_from_database(table_name)


@mark_with_dialects_excluding()
@mark.needs_pandas
def test_register_pandas_data(dialect, test_helpers):
    import pandas as pd

    helper = test_helpers[dialect]

    test_data_pd = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    table_name = "test_table"
    db_api.register_table(
        input_table=test_data_pd,
        table_name=table_name,
    )
    db_api.delete_table_from_database(table_name)


@mark_with_dialects_excluding()
def test_register_arrow_data(dialect, test_helpers):
    helper = test_helpers[dialect]

    test_data_arrow = pa.Table.from_pydict(
        {
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
        }
    )
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    table_name = "test_table"
    db_api.register_table(
        input_table=test_data_arrow,
        table_name=table_name,
    )
    db_api.delete_table_from_database(table_name)

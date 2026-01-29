from tests.decorator import mark_with_dialects_excluding


def test_splink_dataframe_from_list():
    pass  # TODO

@mark_with_dialects_excluding()
def test_splink_dataframe_to_pyarrow(dialect, test_helpers):
    helper = test_helpers[dialect]

    db_api = helper.db_api()

    input_data = [
        {"id": 1, "backend": "duckdb"},
        {"id": 2, "backend": "spark"},
    ]

    sdf = db_api.register(input_data, "test_input_table")
    sdf.to_pyarrow_table()

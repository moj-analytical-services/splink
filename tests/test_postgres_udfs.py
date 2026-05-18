import pytest

pytest.importorskip("sqlalchemy")
# ruff: noqa: E402 (module level import not at top of file)

from splink.internals.postgres.database_api import PostgresAPI

from .decorator import mark_with_dialects_including


@mark_with_dialects_including("postgres")
def test_log2(pg_engine):
    db_api = PostgresAPI(engine=pg_engine)

    data = [
        {"x": 8, "expected_log2_value": 3},
        {"x": 2, "expected_log2_value": 1},
        {"x": 1, "expected_log2_value": 0},
        {"x": 0.5, "expected_log2_value": -1},
    ]
    log_values = db_api.register(data)

    sql = "SELECT log2(x) AS actual_log2_value, expected_log2_value FROM {this}"
    result = log_values.query_sql(sql)

    for result_row in result.as_record_dict():
        assert result_row["actual_log2_value"] == result_row["expected_log2_value"]


@mark_with_dialects_including("postgres")
def test_array_intersect(pg_engine):
    db_api = PostgresAPI(engine=pg_engine)
    data = [
        {"arr_l": [1, 2, 3], "arr_r": [1, 5, 6], "expected": [1]},
        {"arr_l": [1, 2, 3], "arr_r": [10, 1, -2], "expected": [1]},
        {"arr_l": [1, 2, 3], "arr_r": [4, 5, 6], "expected": []},
        {"arr_l": [1, 2, 3], "arr_r": [2, 1, 7, 10], "expected": [1, 2]},
        {"arr_l": [1, 2, 3], "arr_r": [1, 1, 1], "expected": [1]},
        {"arr_l": [1, 1, 1], "arr_r": [1, 2, 3], "expected": [1]},
        {"arr_l": [3, 5, 7], "arr_r": [3, 5, 7], "expected": [3, 5, 7]},
        {"arr_l": [1, 2, 3, 4, 5], "arr_r": [3, 5, 7], "expected": [3, 5]},
    ]

    data_sdf = db_api.register(data)
    sql = """
        SELECT
            array_intersect(arr_l, arr_r) AS actual_intersection,
            expected AS expected_intersection
        FROM {this}
    """
    result = data_sdf.query_sql(sql)

    for result_row in result.as_record_dict():
        # don't care about order
        assert set(result_row["actual_intersection"]) == set(
            result_row["expected_intersection"]
        )
        # should check we don't have duplicates
        assert len(result_row["actual_intersection"]) == len(
            result_row["expected_intersection"]
        )

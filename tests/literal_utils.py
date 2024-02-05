import json

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

import splink.comparison_level_library as cll
from splink import exceptions
from splink.database_api import DuckDBAPI
from splink.linker import Linker

db_api = DuckDBAPI()


def get_comparison_level(test_spec, test):
    if "comparison_level" in test_spec:
        return test_spec["comparison_level"]
    else:
        kwargs = test_spec.get("kwargs", {})
        kwargs_overrides = test.get("kwargs_overrides", {})
        kwargs.update(kwargs_overrides)
        comparison_level_class = test_spec["comparison_level_class"]
        return comparison_level_class(**kwargs)


def get_sql(comparison_level, db_api):
    sql = comparison_level.create_level_dict(db_api.sql_dialect.sqlglot_name)[
        "sql_condition"
    ]

    return f"select {sql} as in_level from __splink__test_table"


def execute_sql_for_test(sql, db_api):
    return db_api.execute_sql_against_backend(
        sql, "__splink__test", "__splink__test"
    ).as_pandas_dataframe()


def run_tests_with_args(test_spec, db_api):
    tests = test_spec["tests"]
    for test in tests:
        sqlglot_dialects = test.get(
            "sqlglot_dialects", [db_api.sql_dialect.sqlglot_name]
        )
        if db_api.sql_dialect.sqlglot_name not in sqlglot_dialects:
            continue

        comparison_level = get_comparison_level(test_spec, test)
        sql = get_sql(comparison_level, db_api)
        table_as_dict = [test["values"]]
        db_api._delete_table_from_database("__splink__test_table")
        db_api._table_registration(table_as_dict, "__splink__test_table")

        if "expected_exception" in test:
            with pytest.raises(test["expected_exception"]):
                in_level = execute_sql_for_test(sql, db_api).iloc[0, 0]
            continue

        in_level = execute_sql_for_test(sql, db_api).iloc[0, 0]
        assert (
            in_level == test["expected_to_be_in_level"]
        ), f"Failed test: values={json.dumps(test)}, sql={sql}"

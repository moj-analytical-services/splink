import duckdb
import sqlglot
import sqlglot.expressions as exp
import splink.comparison_level_library as cll
from splink import exceptions
import pytest
import json


def populate_identifiers_with_literals(sql, literal_lookup, sqlglot_dialect_name):
    expression_tree = sqlglot.parse_one(sql, dialect=sqlglot_dialect_name)

    def transformer(node):
        if isinstance(node, exp.Identifier) and node.name in literal_lookup:
            return sqlglot.parse_one(literal_lookup[node.name])
        return node

    transformed_tree = expression_tree.transform(transformer)
    return transformed_tree.sql(dialect=sqlglot_dialect_name)


def get_comparison_level(test_spec, test):
    if "comparison_level" in test_spec:
        return test_spec["comparison_level"]
    else:
        kwargs = test_spec.get("kwargs", {})
        kwargs_overrides = test.get("kwargs_overrides", {})
        kwargs.update(kwargs_overrides)
        comparison_level_class = test_spec["comparison_level_class"]
        return comparison_level_class(**kwargs)


def get_sql_with_literals(comparison_level, test, linker):
    sql = comparison_level.create_level_dict(linker.sqlglot_dialect_name)[
        "sql_condition"
    ]
    values_lookup = test["values"]
    sql_with_literals = populate_identifiers_with_literals(
        sql, values_lookup, linker.sqlglot_dialect_name
    )
    return f"select {sql_with_literals} as in_level"


def run_tests_with_args(test_spec, linker):
    tests = test_spec["tests"]
    for test in tests:
        sqlglot_dialects = test.get("sqlglot_dialects", [linker.sqlglot_dialect_name])
        if linker.sqlglot_dialect_name not in sqlglot_dialects:
            continue

        comparison_level = get_comparison_level(test_spec, test)
        sql_with_literals = get_sql_with_literals(comparison_level, test, linker)

        if "expected_exception" in test:
            with pytest.raises(test["expected_exception"]):
                in_level = linker.query_sql(sql_with_literals).iloc[0, 0]
            continue

        in_level = linker.query_sql(sql_with_literals).iloc[0, 0]
        assert (
            in_level == test["expected_to_be_in_level"]
        ), f"Failed test: values={json.dumps(test)}, sql={sql_with_literals}"


# Mocked for now
class DuckDBLinker:
    def __init__(self):
        self.con = duckdb.connect()
        self.sqlglot_dialect_name = "duckdb"

    def query_sql(self, sql):
        return self.con.execute(sql).df()


linker = DuckDBLinker()


def test_datediff_levels():
    test_spec_day_threshold = {
        "comparison_level": cll.DatediffLevel(
            col_name="dob",
            date_threshold=30,
            date_metric="day",
            cast_strings_to_date=False,
        ),
        "tests": [
            {
                "values": {
                    "dob_l": "cast('2020-01-01' as date)",
                    "dob_r": "cast('2020-01-29' as date)",
                },
                "expected_to_be_in_level": True,
            },
            {
                "values": {
                    "dob_l": "cast('2020-01-01' as date)",
                    "dob_r": "cast('2020-02-01' as date)",
                },
                "expected_to_be_in_level": False,
            },
        ],
    }

    run_tests_with_args(test_spec_day_threshold, linker)

    test_spec_month_threshold = {
        "comparison_level": cll.DatediffLevel(
            col_name="dob",
            date_threshold=6,
            date_metric="month",
            cast_strings_to_date=True,
            date_format="%Y-%m-%d",
        ),
        "tests": [
            {
                "values": {
                    "dob_l": "'2020-01-01'",
                    "dob_r": "'2020-06-01'",
                },
                "expected_to_be_in_level": True,
            },
            {
                "values": {
                    "dob_l": "'2020-01-01'",
                    "dob_r": "'2020-08-01'",
                },
                "expected_to_be_in_level": False,
            },
        ],
    }

    run_tests_with_args(test_spec_month_threshold, linker)

    test_spec_year_threshold = {
        "comparison_level": cll.DatediffLevel(
            col_name="dob",
            date_threshold=1,
            date_metric="year",
            cast_strings_to_date=True,
            date_format="%Y-%m-%d",
        ),
        "tests": [
            {
                "values": {
                    "dob_l": "'2020-01-01'",
                    "dob_r": "'2020-12-25'",
                },
                "expected_to_be_in_level": True,
            },
            {
                "values": {
                    "dob_l": "'2020-01-01'",
                    "dob_r": "'2022-01-01'",
                },
                "expected_to_be_in_level": False,
            },
        ],
    }

    run_tests_with_args(test_spec_year_threshold, linker)

    test_spec_year_threshold_with_formats = {
        "comparison_level_class": cll.DatediffLevel,
        "kwargs": {
            "col_name": "dob",
            "date_threshold": 1,
            "date_metric": "year",
            "cast_strings_to_date": True,
        },
        "tests": [
            {
                "kwargs_overrides": {
                    "date_format": "%Y-%m-%d",
                },
                "values": {
                    "dob_l": "'2020-01-01'",
                    "dob_r": "'2020-12-25'",
                },
                "expected_to_be_in_level": True,
            },
            {
                "kwargs_overrides": {
                    "date_format": "%d-%m-%Y",
                },
                "values": {
                    "dob_l": "'01-01-2020'",
                    "dob_r": "'01-02-2022'",
                },
                "expected_to_be_in_level": False,
            },
        ],
    }

    run_tests_with_args(test_spec_year_threshold_with_formats, linker)

    test_spec_exceptions = {
        "comparison_level_class": cll.DatediffLevel,
        "kwargs": {
            "col_name": "dob",
            "date_threshold": 2,
            "date_metric": "day",
            "cast_strings_to_date": True,
        },
        "tests": [
            {
                "sqlglot_dialects": ["postgres", "spark"],
                "kwargs_overrides": {
                    "date_format": "y/M/d",
                },
                "values": {
                    "dob_l": "'2020/12/25'",
                    "dob_r": "'2020/12/25'",
                },
                "expected_exception": Exception,
            },
            {
                "sqlglot_dialects": ["postgres", "spark"],
                "kwargs_overrides": {
                    "date_format": "y/M/d",
                },
                "values": {
                    "dob_l": "'2020/12/25'",
                    "dob_r": "'2020/12/25'",
                },
                "expected_to_be_in_level": True,
            },
            {
                "sqlglot_dialects": ["duckdb"],
                "kwargs_overrides": {
                    "date_format": "%d/%m/%Y",
                },
                "values": {
                    "dob_l": "'2020-12-25'",
                    "dob_r": "'2020-12-25'",
                },
                "expected_exception": Exception,
            },
            {
                "sqlglot_dialects": ["duckdb"],
                "kwargs_overrides": {
                    "date_format": "%d/%m/%Y",
                },
                "values": {
                    "dob_l": "'25/12/2020'",
                    "dob_r": "'25/12/2020'",
                },
                "expected_to_be_in_level": True,
            },
        ],
    }

    run_tests_with_args(test_spec_exceptions, linker)

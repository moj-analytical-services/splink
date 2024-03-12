import json
from typing import List, Type, Union

import pytest

from splink.comparison_creator import ComparisonCreator
from splink.comparison_level_creator import ComparisonLevelCreator


class ComparisonLevelTestSpec:
    def __init__(
        self,
        comparison_level_or_class: Union[
            ComparisonLevelCreator, Type[ComparisonLevelCreator]
        ],
        tests: List,
        default_keyword_args: dict = None,
    ):
        self.tests = tests
        self.comparison_level_or_class = comparison_level_or_class

        self.default_keyword_args = (
            {} if default_keyword_args is None else default_keyword_args
        )

    @property
    def has_class_not_instance(self) -> bool:
        if isinstance(self.comparison_level_or_class, type) and issubclass(
            self.comparison_level_or_class, ComparisonLevelCreator
        ):
            return True
        else:
            return False

    @property
    def comparison_level_creator(self) -> ComparisonLevelCreator:
        if self.has_class_not_instance:
            return self.comparison_level_or_class(**self.keyword_args_combined)
        else:
            return self.comparison_level_or_class

    def get_sql(self, sqlglot_name):
        sql = self.comparison_level_creator.get_comparison_level(
            sqlglot_name
        ).sql_condition

        return f"select {sql} as in_level from __splink__test_table"

    @property
    def keyword_args_combined(self):
        return {**self.default_keyword_args, **self.keyword_arg_overrides}


class ComparisonTestSpec:
    def __init__(
        self,
        comparison_or_class: Union[ComparisonCreator, Type[ComparisonCreator]],
        tests: List,
        default_keyword_args: dict = None,
    ):
        self.tests = tests
        self.comparison_level_or_class = comparison_or_class

        self.default_keyword_args = (
            {} if default_keyword_args is None else default_keyword_args
        )

        self.keyword_arg_overrides = {}

    @property
    def has_class_not_instance(self) -> bool:
        if isinstance(self.comparison_level_or_class, type) and issubclass(
            self.comparison_level_or_class, ComparisonCreator
        ):
            return True
        else:
            return False

    @property
    def comparison_creator(self) -> ComparisonCreator:
        if self.has_class_not_instance:
            return self.comparison_level_or_class(**self.keyword_args_combined)
        else:
            return self.comparison_level_or_class

    def get_sql(self, sqlglot_name):
        c = self.comparison_creator.get_comparison(sqlglot_name)
        sqls = [cl._when_then_comparison_vector_value_sql for cl in c.comparison_levels]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END "
        return f"select {sql} as gamma_value from __splink__test_table"

    @property
    def keyword_args_combined(self):
        return {**self.default_keyword_args, **self.keyword_arg_overrides}


def execute_sql_for_test(sql, db_api):
    return db_api.execute_sql_against_backend(
        sql, "__splink__test", "__splink__test"
    ).as_pandas_dataframe()


def run_tests_with_args(
    test_spec: Union[ComparisonLevelTestSpec, ComparisonTestSpec], db_api
):
    tests = (
        test_spec.tests
    )  # Assuming tests are now a list of LiteralTestValues objects
    sqlglot_name = db_api.sql_dialect.sqlglot_name
    for test in tests:
        if test.sql_dialects:
            if sqlglot_name not in test.sql_dialects:
                continue
        if test.keyword_arg_overrides:
            test_spec.keyword_arg_overrides = test.keyword_arg_overrides
        else:
            test_spec.keyword_arg_overrides = {}

        sql = test_spec.get_sql(sqlglot_name)

        # Adjust to the structure of LiteralTestValues

        table_as_dict = test.vals_for_df
        if db_api.table_exists_in_database("__splink__test_table"):
            db_api._delete_table_from_database("__splink__test_table")

        db_api._table_registration(table_as_dict, "__splink__test_table")

        if test.expected_exception:
            with pytest.raises(test.expected_exception):
                actual_value = execute_sql_for_test(sql, db_api).iloc[0, 0]
            continue

        actual_value = execute_sql_for_test(sql, db_api).iloc[0, 0]

        # Determine the expected result based on the type of test_spec
        if isinstance(test_spec, ComparisonTestSpec):
            expected_result = test.expected_gamma_val
        else:  # Assuming it's ComparisonLevelTestSpec or similar
            expected_result = test.expected_in_level

        assert actual_value == expected_result, (
            f"Failed test: actual_value={actual_value} expected_value={expected_result}"
            f" values={json.dumps(test.values)}, sql={sql}"
        )


class LiteralTestValues:
    def __init__(
        self,
        values,
        *,
        expected_in_level=None,
        expected_gamma_val=None,
        sql_dialects=None,
        keyword_arg_overrides=None,
        expected_exception=None,
    ):
        self.values = values

        self.expected_in_level = expected_in_level
        self.expected_gamma_val = expected_gamma_val
        self.sql_dialects = sql_dialects
        self.keyword_arg_overrides = keyword_arg_overrides
        self.expected_exception = expected_exception

    @property
    def vals_for_df(self):
        return [self.values]

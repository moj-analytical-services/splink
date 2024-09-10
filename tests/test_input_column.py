from dataclasses import dataclass
from functools import partial

import pytest

from splink.internals.input_column import InputColumn, _get_dialect_quotes


@dataclass
class ColumnTestCase:
    """A simple helper class to more easily faciliate a range of
    tests on expected outputs from our array of InputColumn methods.

    - input_column: name of your initial column
    - name_out: The expected name, with placeholders
    - alias: the alias in an `AS ...` statement
    - sql_dialect: The dialect to use.
    """

    input_column: str
    name_out: str
    alias: str
    sql_dialect: str

    def __post_init__(self):
        # Retrieve dialect quotes
        dialect_quotes, q = _get_dialect_quotes(self.sql_dialect)

        # Format strings with dialect quotes
        self.name_out = self.name_out.format(q=dialect_quotes)
        self.name_with_table = q + "{table}" + q + "." + self.name_out
        self.alias = self.alias.format(q=dialect_quotes, prefix="", suffix="")

        # Format input_column with the formatted name
        name = self.input_column.format(q=dialect_quotes)
        self.input_column = InputColumn(name, sqlglot_dialect_str=self.sql_dialect)

    def expected_name(self, prefix: str, suffix: str):
        return self.name_out.format(prefix=prefix, suffix=suffix)

    def expected_table_and_alias(self, table: str, prefix: str, suffix: str):
        table = self.name_with_table.format(table=table, prefix=prefix, suffix="")
        alias = self.alias.format(prefix=prefix, suffix=suffix)
        return f"{table} AS {alias}"


def test_input_column():
    c = InputColumn("my_col", sqlglot_dialect_str="duckdb")
    assert c.name == '"my_col"'
    # Check we only unquote for a given column building instance
    assert c.unquote().name == "my_col"
    # Quotes should now return...
    assert c.name_l == '"my_col_l"'
    assert c.tf_name_l == '"tf_my_col_l"'
    assert c.l_tf_name_as_l == '"l"."tf_my_col" AS "tf_my_col_l"'
    # Removes quotes for table name, column name and the alias
    assert c.unquote().l_tf_name_as_l == "l.tf_my_col AS tf_my_col_l"

    c = InputColumn("SUR name", sqlglot_dialect_str="duckdb")
    assert c.name == '"SUR name"'
    assert c.name_r == '"SUR name_r"'
    assert c.r_name_as_r == '"r"."SUR name" AS "SUR name_r"'

    c = InputColumn("col['lat']", sqlglot_dialect_str="duckdb")

    identifier = """
    "col"['lat']
    """.strip()
    assert c.name == identifier

    l_tf_name_as_l = """
    "l"."tf_col"['lat'] AS "tf_col_l['lat']"
    """.strip()
    assert c.l_tf_name_as_l == l_tf_name_as_l

    assert c.unquote().name == "col['lat']"

    c = InputColumn("first name", sqlglot_dialect_str="spark")
    assert c.name == "`first name`"


@pytest.mark.parametrize("dialect", ["spark", "duckdb"])
def test_input_column_without_expressions(dialect):
    ColumnTester = partial(ColumnTestCase, sql_dialect=dialect)

    # dir indicates the direction to be used for replacement
    test_cases = (
        ColumnTester(
            # With raw identifier
            input_column="test",
            name_out="{q}{{prefix}}test{{suffix}}{q}",
            alias="{q}{{prefix}}test{{suffix}}{q}",
        ),
        ColumnTester(
            # With a str bracket index
            input_column="test['lat']",
            name_out="{q}{{prefix}}test{{suffix}}{q}['lat']",
            alias="{q}{{prefix}}test{{suffix}}['lat']{q}",
        ),
        ColumnTester(
            # With spacey name + str bracket index
            input_column="full name['surname']",
            name_out="{q}{{prefix}}full name{{suffix}}{q}['surname']",
            alias="{q}{{prefix}}full name{{suffix}}['surname']{q}",
        ),
        ColumnTester(
            # With an int bracket index
            input_column="test[0]",
            name_out="{q}{{prefix}}test{{suffix}}{q}[0]",
            alias="{q}{{prefix}}test{{suffix}}[0]{q}",
        ),
        ColumnTester(
            # With spacey identifier
            input_column="sur name",
            name_out="{q}{{prefix}}sur name{{suffix}}{q}",
            alias="{q}{{prefix}}sur name{{suffix}}{q}",
        ),
        ColumnTester(
            # Spacey identifier + quotes
            input_column="{q}sur name{q}",
            name_out="{q}{{prefix}}sur name{{suffix}}{q}",
            alias="{q}{{prefix}}sur name{{suffix}}{q}",
        ),
        ColumnTester(
            # Illegal name in sqlglot
            input_column="first]name",
            name_out="{q}{{prefix}}first]name{{suffix}}{q}",
            alias="{q}{{prefix}}first]name{{suffix}}{q}",
        ),
        ColumnTester(
            # SQL key argument
            input_column="group",
            name_out="{q}{{prefix}}group{{suffix}}{q}",
            alias="{q}{{prefix}}group{{suffix}}{q}",
        ),
    )

    # The following variation matrix only works because our `InputColumn` method
    # names match the expected outputs,
    # For example - Property name:`col.name_l` -> Expected output:`{col}_l`.
    # If we change our method names, we will need to tweak how this operates.
    table_prefix_suffix_variations = (
        # table, prefix, suffix
        ("l", "", "_l"),
        ("r", "", "_r"),
        ("l", "tf_", "_l"),  # tf
        ("r", "tf_", "_r"),  # tf
    )

    for column in test_cases:
        for table, prefix, suffix in table_prefix_suffix_variations:
            # Input Column
            input_column = column.input_column
            # Return values
            expected_name_l_r = column.expected_name(prefix, suffix)
            expected_name_with_table_alias = column.expected_table_and_alias(
                table, prefix, suffix
            )
            # Property names
            column_prefix_suffix_method = f"{prefix}name{suffix}"
            column_table_prefix_suffix_method = f"{table}_{prefix}name_as{suffix}"

            # Check every combination is as we expect
            assert (
                getattr(input_column, column_prefix_suffix_method) == expected_name_l_r
            )
            assert (
                getattr(input_column, column_table_prefix_suffix_method)
                == expected_name_with_table_alias
            )


def test_illegal_names_error():
    # Check some odd, but legal names all run without issue
    odd_but_legal_names = (
        "sur[test",
        "sur#test",
        "sur 'name'",
        "sur[test['lat']",
        "sur'name",
        "sur,name",
        "sur  name",
        "my test column",
    )
    for name in odd_but_legal_names:
        InputColumn(name, sqlglot_dialect_str="duckdb").name_l  # noqa: B018

    # Check some illegal names we want to raise ParserErrors
    illegal_names = ('sur "name"', '"sur" name', '"sur" name[0]', "sur \"name\"['lat']")
    for name in illegal_names:
        with pytest.raises((ValueError)):
            InputColumn(name, sqlglot_dialect_str="duckdb")

    # TokenError
    token_errors = ('"sur" name"', 'sur"name')
    for name in token_errors:
        with pytest.raises(ValueError):
            InputColumn(name, sqlglot_dialect_str="duckdb")

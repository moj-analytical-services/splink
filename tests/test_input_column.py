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


@pytest.mark.parametrize("dialect", ["spark", "duckdb", "postgres"])
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
            input_column="test[1]",
            name_out="{q}{{prefix}}test{{suffix}}{q}[1]",
            alias="{q}{{prefix}}test{{suffix}}[1]{q}",
        ),
        ColumnTester(
            # With a negative int bracket index
            input_column="test[-1]",
            name_out="{q}{{prefix}}test{{suffix}}{q}[-1]",
            alias="{q}{{prefix}}test{{suffix}}[-1]{q}",
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


def test_input_column_equality():
    """Test that InputColumns can be compared for equality regardless of quoting."""
    # Basic equality - same column, different quoting
    col_quoted = InputColumn("my_col", sqlglot_dialect_str="duckdb")
    col_unquoted = col_quoted.unquote()

    assert col_quoted == col_unquoted
    assert col_unquoted == col_quoted

    # Same column created separately
    col1 = InputColumn("first_name", sqlglot_dialect_str="duckdb")
    col2 = InputColumn("first_name", sqlglot_dialect_str="duckdb")
    assert col1 == col2

    # Different columns should not be equal
    col_a = InputColumn("col_a", sqlglot_dialect_str="duckdb")
    col_b = InputColumn("col_b", sqlglot_dialect_str="duckdb")
    assert col_a != col_b

    # Columns with spaces - quoted vs unquoted should be equal
    col_space = InputColumn("first name", sqlglot_dialect_str="duckdb")
    col_space_unquoted = col_space.unquote()
    assert col_space == col_space_unquoted

    # Struct columns with bracket keys should be equal regardless of quoting
    col_struct = InputColumn("col['lat']", sqlglot_dialect_str="duckdb")
    col_struct_unquoted = col_struct.unquote()
    assert col_struct == col_struct_unquoted

    # Different bracket keys should not be equal
    col_lat = InputColumn("col['lat']", sqlglot_dialect_str="duckdb")
    col_lon = InputColumn("col['lon']", sqlglot_dialect_str="duckdb")
    assert col_lat != col_lon

    # Array columns with bracket indices should be equal regardless of quoting
    col_arr = InputColumn("col[1]", sqlglot_dialect_str="duckdb")
    col_arr_unquoted = col_arr.unquote()
    assert col_arr == col_arr_unquoted

    # Different bracket indices should not be equal
    col_0 = InputColumn("col[1]", sqlglot_dialect_str="duckdb")
    col_1 = InputColumn("col[2]", sqlglot_dialect_str="duckdb")
    assert col_0 != col_1

    # Equality with non-InputColumn should return NotImplemented (handled by Python)
    col = InputColumn("my_col", sqlglot_dialect_str="duckdb")
    assert col != "my_col"
    assert col != 42


def test_input_column_in_operator():
    """Test that InputColumns work correctly with the `in` operator."""
    col1 = InputColumn("first_name", sqlglot_dialect_str="duckdb")
    col2 = InputColumn("last_name", sqlglot_dialect_str="duckdb")
    col3 = InputColumn("dob", sqlglot_dialect_str="duckdb")

    cols_list = [col1, col2]

    # Same column should be found in list
    assert col1 in cols_list
    assert col2 in cols_list
    assert col3 not in cols_list

    # Unquoted version should also be found
    col1_unquoted = col1.unquote()
    assert col1_unquoted in cols_list

    # New instance of same column should be found
    col1_new = InputColumn("first_name", sqlglot_dialect_str="duckdb")
    assert col1_new in cols_list


def test_input_column_hashable():
    """Test that InputColumns can be used in sets and as dict keys."""
    col1 = InputColumn("first_name", sqlglot_dialect_str="duckdb")
    col1_unquoted = col1.unquote()
    col2 = InputColumn("last_name", sqlglot_dialect_str="duckdb")

    # Can create a set of InputColumns
    col_set = {col1, col2}
    assert len(col_set) == 2

    # Adding the unquoted version shouldn't increase the set size
    col_set.add(col1_unquoted)
    assert len(col_set) == 2

    # Can check membership in sets
    assert col1 in col_set
    assert col1_unquoted in col_set
    assert col2 in col_set

    # Can use as dict keys
    col_dict = {col1: "first", col2: "last"}
    assert col_dict[col1] == "first"
    assert col_dict[col1_unquoted] == "first"  # Same key despite quoting difference

    # Struct columns with bracket keys
    col_struct = InputColumn("coords['lat']", sqlglot_dialect_str="duckdb")
    col_struct_unquoted = col_struct.unquote()
    struct_set = {col_struct}
    assert col_struct_unquoted in struct_set

    # Array columns with bracket indices
    col_arr = InputColumn("items[1]", sqlglot_dialect_str="duckdb")
    col_arr_unquoted = col_arr.unquote()
    arr_set = {col_arr}
    assert col_arr_unquoted in arr_set

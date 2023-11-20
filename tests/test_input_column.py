from dataclasses import dataclass
from functools import partial

import pytest
from sqlglot.errors import ParseError, TokenError

from splink.input_column import InputColumn, _get_dialect_quotes


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
    sql_dialect: str  # Assuming sql_dialect is passed as an argument

    def __post_init__(self):
        # Retrieve dialect quotes
        dialect_quotes, q = _get_dialect_quotes(self.sql_dialect)

        # Format strings with dialect quotes
        self.name_out = self.name_out.format(q=dialect_quotes)
        self.name_with_table = q + "{table}" + q + "." + self.name_out
        self.alias = self.alias.format(q=dialect_quotes, prefix="", suffix="")

        # Format input_column with the formatted name
        name = self.input_column.format(q=dialect_quotes)
        self.input_column = InputColumn(name, sql_dialect=self.sql_dialect)

    def expected_name(self, prefix: str, suffix: str):
        return self.name_out.format(prefix=prefix, suffix=suffix)

    def expected_table_and_alias(self, table: str, prefix: str, suffix: str):
        table = self.name_with_table.format(table=table, prefix=prefix, suffix="")
        alias = self.alias.format(prefix=prefix, suffix=suffix)
        return f"{table} AS {alias}"


def test_input_column():
    c = InputColumn("my_col")
    assert c.name == '"my_col"'
    # Check we only unquote for a given column building instance
    assert c.unquote().name == "my_col"
    # Quotes should now return...
    assert c.name_l == '"my_col_l"'
    assert c.tf_name_l == '"tf_my_col_l"'
    assert c.l_tf_name_as_l == '"l"."tf_my_col" AS "tf_my_col_l"'
    # Removes quotes for table name, column name and the alias
    assert c.unquote().l_tf_name_as_l == "l.tf_my_col AS tf_my_col_l"

    c = InputColumn("SUR name")
    assert c.name == '"SUR name"'
    assert c.name_r == '"SUR name_r"'
    assert c.r_name_as_r == '"r"."SUR name" AS "SUR name_r"'

    c = InputColumn("col['lat']")

    identifier = """
    "col"['lat']
    """.strip()
    assert c.name == identifier

    l_tf_name_as_l = """
    "l"."tf_col"['lat'] AS "tf_col_l['lat']"
    """.strip()
    assert c.l_tf_name_as_l == l_tf_name_as_l

    assert c.unquote().name == "col['lat']"

    c = InputColumn("first name", sql_dialect="spark")
    assert c.name == "`first name`"

    # Check if adding a bracket index works for illegal names:
    # - sur name['lat'] is illegal without brackets around "sur name"
    col = InputColumn("sur name")
    col.add_bracket_index_to_col("lat")
    assert col.name == "\"sur name\"['lat']"

    col = InputColumn("sur name")
    col.add_bracket_index_to_col("0")  # int is cast as expected
    assert col.name == '"sur name"[0]'


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


def test_input_column_with_expressions():
    indexed_column = "test['lat']"
    indexed_input_column = InputColumn(indexed_column, sql_dialect="duckdb")
    # Add lowercase and regex
    indexed_input_column.lowercase_column(True).regex_extract("\\d+")

    regex_to_lower = r"""
    REGEXP_EXTRACT(LOWER("l"."test"['lat']), '\d+', 0) AS "test_l['lat']"
    """.strip()
    assert indexed_input_column.l_name_as_l == regex_to_lower

    # Reset and add: str_to_date -> lower
    indexed_input_column.reset_expressions
    indexed_input_column.lowercase_column(True).str_to_date(True, "dd mm yy")

    strptime_to_lower = r"""
    STRPTIME(LOWER("test_l"['lat']), 'dd mm yy')
    """.strip()
    assert indexed_input_column.name_l == strptime_to_lower

    indexed_input_column.lowercase_column(True)
    lower_strptime = r"""
    LOWER(STRPTIME("test_l"['lat'], 'dd mm yy'))
    """.strip()
    assert indexed_input_column.name_l == lower_strptime
    # Test reordering the str_to_date - we expect str_to_date to now be the outermost fn
    assert (
        indexed_input_column.str_to_date(True, "dd mm yy").name_l == strptime_to_lower
    )

    # Check that excluding expressions causes only the function to be produced
    assert (
        indexed_input_column.exclude_expressions().unquote().name_l == "test_l['lat']"
    )

    indexed_column = "sur name"
    spacey_input_name = InputColumn(indexed_column, sql_dialect="postgres")

    spacey_input_name.str_to_date(True, "dd-mmm-yyyy")

    postgres_to_date = r"""
    TO_DATE("sur name_r", 'dd-mmm-yyyy')
    """.strip()
    assert spacey_input_name.name_r == postgres_to_date
    assert (
        spacey_input_name.lowercase_column(True).name_r == f"LOWER({postgres_to_date})"
    )

    # Dialect agnostic functions should work as expected
    assert InputColumn("test").lowercase_column(True).name == 'LOWER("test")'

    # ERRORS: Check we get errors when an no dialect or an invalid dialect is passed
    with pytest.raises(ValueError) as exc_info:
        spacey_input_name.regex_extract("\\d+")
    assert str(exc_info.value) == "Dialect 'postgres' does not support regex_extract"

    with pytest.raises(ValueError) as exc_info:
        InputColumn("test").regex_extract("\\d+")
    assert str(exc_info.value).startswith("'regex_extract' requires a valid dialect")
    with pytest.raises(ValueError) as exc_info:
        InputColumn("test").str_to_date(True, "dd-mm-yy").name
    assert str(exc_info.value).startswith("'str_to_date' requires a valid dialect")


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
        InputColumn(name).name_l

    # Check some illegal names we want to raise ParserErrors
    illegal_names = ('sur "name"', '"sur" name', '"sur" name[0]', "sur \"name\"['lat']")
    for name in illegal_names:
        with pytest.raises(ParseError):
            InputColumn(name)

    # TokenError
    token_errors = ('"sur" name"', 'sur"name')
    for name in token_errors:
        with pytest.raises(TokenError):
            InputColumn(name)

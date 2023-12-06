import re
import string
from functools import partial

import sqlglot

from .dialects import SplinkDialect
from .input_column import SqlglotColumnTreeBuilder
from .sql_transform import add_suffix_to_all_column_identifiers


class InputExpression:
    """
    Enables transforms to be applied to a column before it's passed into a
    comparison level.

    Dialect agnostic.  Execution is delayed until the dialect is known.

    For example:
        from splink.input_expression import InputExpression
        col = (
            InputExpression("first_name")
            .lower()
            .regex_extract("^[A-Z]{1,4}")
        )

        ExactMatchLevel(col)

    Note that this will typically be created without a dialect, and the dialect
    will later be populated when the InputExpression is passed via a comparison
    level creator into a linker.
    """

    def __init__(self, sql_expression: str, sql_dialect: SplinkDialect = None):
        self.raw_sql_expression = sql_expression
        self.operations = []
        if sql_dialect is not None:
            self.sql_dialect: SplinkDialect = sql_dialect

    def parse_input_string(self, dialect: SplinkDialect):
        """
        The input into an InputExpression can be
            - a column name or column reference e.g. first_name, first name
            - a sql expression e.g. UPPER(first_name), first_name || surname

        In the former case, we do not expect the user to have escaped the column name
        with identifier quotes (see also InputColumn).

        In the later case, we expect the expression to be valid sql in the dialect
        that the user will specify in their linker.
        """

        # It's difficult (possibly impossible) to find a completely general
        # algorithm that can distinguish between the two cases (col name, or sql
        # expression), since lower(first_name) could technically be a column name
        # Here I use a heuristic:
        # If there's a () or || then assume it's a sql expression
        if re.search(r"\([^)]*\)", self.raw_sql_expression):
            return self.raw_sql_expression
        elif "||" in self.raw_sql_expression:
            return self.raw_sql_expression

        # Otherwise, assume it's a column name or reference which may need quoting
        return SqlglotColumnTreeBuilder.from_raw_column_name_or_column_reference(
            self.raw_sql_expression, dialect.sqlglot_name
        ).sql

    @property
    def is_pure_column_or_column_reference(self):
        if len(self.operations) > 0:
            return False

        if re.search(r"\([^)]*\)", self.raw_sql_expression):
            return False

        if "||" in self.raw_sql_expression:
            return False

        return True

    def apply_operations(self, name, dialect):
        for op in self.operations:
            name = op(name=name, dialect=dialect)
        return name

    def _lower_dialected(self, name, dialect):
        lower_sql = sqlglot.parse_one("lower(___col___)").sql(
            dialect=dialect.sqlglot_name
        )

        return lower_sql.replace("___col___", name)

    def lower(self):
        """
        Applies a lowercase transofrom to the input expression.
        """
        self.operations.append(self._lower_dialected)
        return self

    def _substr_dialected(self, name, start, end, dialect):
        substr_sql = sqlglot.parse_one(f"substring(___col___, {start}, {end})").sql(
            dialect=dialect.sqlglot_name
        )

        return substr_sql.replace("___col___", name)

    def substr(self, start: int, length: int):
        """
        Applies a substring transform to the input expression of a given length
        starting from a specified index.
        Args:
            start (int): The starting index of the substring.
            length (int): The length of the substring.
        """
        op = partial(self._substr_dialected, start=start, end=length)
        self.operations.append(op)

        return self

    def _regex_replace_dialected(
        self,
        name: str,
        pattern: str,
        replacement: str,
        capture_group: int,
        dialect: SplinkDialect,
    ) -> str:
        if capture_group is not None:
            cg = capture_group
            sql = f"regexp_replace(___col___, '{pattern}', '{replacement}', {cg})"
        else:
            sql = f"regexp_replace(___col___, '{pattern}', '{replacement}')"

        regex_replace_sql = sqlglot.parse_one(sql).sql(dialect=dialect.sqlglot_name)

        return regex_replace_sql.replace("___col___", name)

    def regex_replace(self, pattern: str, replacement: str, capture_group: int = 1):
        """Applies a regex replace transform to the input expression.

        Args:
            pattern (str): The regex pattern to match.
            replacement (str): The string to replace the matched pattern with.
            capture_group (int): The capture group to extract from the matched pattern.

        """
        op = partial(
            self._regex_extract_dialected,
            pattern=pattern,
            replacement=replacement,
            capture_group=capture_group,
        )
        self.operations.append(op)

        return self

    def _regex_extract_dialected(
        self,
        name: str,
        pattern: str,
        dialect: SplinkDialect,
    ) -> str:
        # TODO - add support for capture group.  This will require
        # adding dialect specific functions because sqlglot doesn't yet support the
        # position (capture group) arg.
        sql = f"regexp_extract(___col___, '{pattern}')"
        regex_extract_sql = sqlglot.parse_one(sql).sql(dialect=dialect.sqlglot_name)

        return regex_extract_sql.replace("___col___", name)

    def regex_extract(self, pattern: str, capture_group: int = 1):
        """Applies a regex extract transform to the input expression.

        Args:
            pattern (str): The regex pattern to match.
            capture_group (int): The capture group to extract from the matched pattern.

        """
        op = partial(
            self._regex_extract_dialected,
            pattern=pattern,
        )
        self.operations.append(op)

        return self

    @property
    def name(self) -> str:
        sql_expression = self.parse_input_string(self.sql_dialect)
        return self.apply_operations(sql_expression, self.sql_dialect)

    @property
    def name_l(self) -> str:
        sql_expression = self.parse_input_string(self.sql_dialect)

        base_name = add_suffix_to_all_column_identifiers(
            sql_expression, "_l", self.sql_dialect.sqlglot_name
        )
        return self.apply_operations(base_name, self.sql_dialect)

    @property
    def name_r(self) -> str:
        sql_expression = self.parse_input_string(self.sql_dialect)
        base_name = add_suffix_to_all_column_identifiers(
            sql_expression, "_r", self.sql_dialect.sqlglot_name
        )
        return self.apply_operations(base_name, self.sql_dialect)

    @property
    def output_column_name(self) -> str:
        allowed_chars = string.ascii_letters + string.digits + "_"
        sanitised_name = "".join(
            c for c in self.raw_sql_expression if c in allowed_chars
        )
        return sanitised_name

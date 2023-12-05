from functools import partial

import sqlglot

from .dialects import SplinkDialect
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

    def __init__(self, sql_expression: str):
        self.sql_expression = sql_expression
        self.operations = []

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

    def _name_l(self, sql_dialect_str: str):
        dialect = SplinkDialect.from_string(sql_dialect_str)
        base_name = add_suffix_to_all_column_identifiers(
            self.sql_expression, "_l", dialect.sqlglot_name
        )
        return self.apply_operations(base_name, dialect)

    def _name_r(self, sql_dialect_str: str):
        dialect = SplinkDialect.from_string(sql_dialect_str)
        base_name = add_suffix_to_all_column_identifiers(
            self.sql_expression, "_r", dialect.sqlglot_name
        )
        return self.apply_operations(base_name, dialect)

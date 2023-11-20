from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import List

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError, TokenError

from .comparison_helpers_utils import unsupported_splink_dialect_expression
from .default_from_jsonschema import default_value_from_schema
from .dialects import SplinkDialect
from .misc import is_castable_to_int


@dataclass
class ColumnBuilder:
    """A builder class that allows you to copy and modify an input column.

    Modifications copy the base column and can be chained to produce a fresh
    version of the column.

    Columns can also be wrapped in SQLglot expressions by appending them to the
    base class.
    """

    name: str
    table: str = None
    quoted: bool = True
    bracket_index: exp.Bracket = None  # the JSON index in 'surname['lat']'
    column_expressions: List[exp.Expression] = field(default_factory=list)
    return_column_with_expressions: bool = True

    def unquote(self):
        return replace(self, quoted=False)

    def exclude_expressions(self):
        return replace(self, return_column_with_expressions=False)

    def change_name(self, new_name: str):
        return replace(self, name=new_name)

    def add_prefix(self, prefix: str):
        return replace(self, name=prefix + self.name) if prefix else self

    def add_suffix(self, suffix: str):
        return replace(self, name=self.name + suffix) if suffix else self

    def add_table(self, table: str):
        return replace(self, table=table) if table else self

    def add_alias(self, alias: str) -> exp.Alias:
        """Alias expects a SQLglot Identifier class or a str.

        For safety, only pass strings to ensure we capture the entire
        column reference, not just the identifier.

        For example, col['lat'] is made up of both an identifier and bracket
        index; the latter of which would be lost if passing identifiers.
        """
        # The identifier should be quoted or unquoted before being passed.
        # This is to ensure we get the correct output.
        alias = exp.Identifier(this=alias, quoted=self.quoted)
        return self.build_column_tree_with_expressions().as_(alias)

    def add_bracket_index(self, index_name: str):
        is_string = not is_castable_to_int(index_name)
        literal = exp.Literal(this=index_name, is_string=is_string)

        self.bracket_index = exp.Bracket(expressions=[literal])
        return self

    def append_expression(self, expression: exp.Expression):
        self.column_expressions.append(expression)
        self.remove_duplicate_expressions()
        return self

    def remove_duplicate_expressions(self):
        """
        Remove duplicate expressions from the column expressions list.

        Ordering is determined by the sequence of appends; the most recently added
        items to the list are retained and appear as the outermost branches
        in the SQL expression tree.
        """
        seen_types = set()
        # Iterate from right to left
        for index in range(len(self.column_expressions) - 1, -1, -1):
            if type(self.column_expressions[index]) in seen_types:
                del self.column_expressions[index]
            else:
                seen_types.add(type(self.column_expressions[index]))

        return self.column_expressions

    @property
    def column_tree(self):
        # Create our column representation
        column = sqlglot.column(col=self.name, table=self.table, quoted=self.quoted)
        # Column is made up of the identifier + bracket index (if it exists)
        if self.bracket_index:
            self.bracket_index.set("this", column)
            return self.bracket_index
        else:
            return column

    def build_column_tree_with_expressions(self) -> sqlglot.Expression:
        """Constructs an SQLglot expression tree by combining a column expression
        with a sequence of SQLglot expressions.

        This method iteratively sets the column value for each expression,
        starting from the provided column and extending to the most recently
        added expression.

        Expressions are modified in-place to avoid copying. This only works
        for expressions expecting a single child - i.e. any value that only
        expects a single "this" key.
        """
        column = self.column_tree
        column_expressions = deepcopy(self.column_expressions)

        if column_expressions and self.return_column_with_expressions:
            # Recursively select the innermost element of the expression tree and
            # add it to our sqlglot column through a simple find and replace.

            # This is possible as our expressions are all functions expecting a single
            # column argument, which we can add a placeholder for and replace.
            for expression in column_expressions:
                expression.find(exp.Column).replace(column)
                # Update the column
                column = expression

        return column

    @classmethod
    def with_sqlglot_column(cls, column: exp.Column, bracket_index: exp.Bracket = None):
        return cls(column.name, bracket_index=bracket_index)

    def __repr__(self):
        # Extract the types of expressions within self.expressions
        expression_types = [type(expr).__name__ for expr in self.column_expressions]

        return (
            f"{self.__class__.__name__}("
            f"name='{self.name}', Quoted={self.quoted}, "
            f"bracket_index={self.bracket_index}, "
            f"expression_types={expression_types})"
        )


class InputColumn:
    """
    Represents a SQL column or column reference
    Handles SQL dialect-specific issues such as identifier quoting.

    The input can be either the raw identifier, or an identifier with
    SQL-specific identifier quotes.

    Examples of valid inputs include:
    - 'first_name'
    - 'first[name'
    - '"first name"'
    - 'coordinates['lat']'
    - '"sur NAME"['lat']
    - 'coordinates[1]'
    """

    def __init__(
        self,
        raw_column_name_or_column_reference: str,
        settings_obj=None,
        sql_dialect: str | SplinkDialect = None,  # TODO: should this be a required arg?
    ):
        # If settings_obj is None, then default values will be used
        # from the jsonschama
        self._settings_obj = settings_obj

        self.register_dialect(sql_dialect)

        self.column_builder = self._parse_input_name_to_sqlglot_tree(
            raw_column_name_or_column_reference
        )

    def register_dialect(self, sql_dialect: str | SplinkDialect):
        if not sql_dialect and self._settings_obj:
            sql_dialect = self._settings_obj._sql_dialect

        if type(sql_dialect) == str:
            sql_dialect = SplinkDialect.from_string(sql_dialect)

        self._sql_dialect = sql_dialect
        self.sqlglot_name = sql_dialect.sqlglot_name if sql_dialect else None

    def from_settings_obj_else_default(self, key, schema_key=None):
        # Covers the case where no settings obj is set on the comparison level
        if self._settings_obj:
            return getattr(self._settings_obj, key)
        else:
            if not schema_key:
                schema_key = key
            return default_value_from_schema(schema_key, "root")

    def _parse_input_name_to_sqlglot_tree(self, name: str) -> ColumnBuilder:
        """
        Parses the input name into a SQLglot expression tree.

        Fiddly because we need to deal with escaping issues.  For example
        the column name in the input dataset may be 'first and surname', but
        if we naively parse this using sqlglot it will be interpreted as an AND
        expression

        Note: We do not support inputs like 'SUR name[1]', in this case the user
        would have to quote e.g. `SUR name`[1]
        """
        q_s, q_e = _get_dialect_quotes(self.sqlglot_name)

        try:
            tree = sqlglot.parse_one(name, read=self.sqlglot_name)
        except (ParseError, TokenError):
            # The parse statement will error in cases such as: sur "name"['lat']
            sqlglot.parse_one(f"{q_s}{name}{q_e}", read=self.sqlglot_name)
            identifier, index = self.manually_split_identifier_and_index(name)

            # Construct the column manually if it can be parsed with quotes.
            # This is useful for illegal columns such as test[ing, test'ing, test"ing
            if index:
                return ColumnBuilder(identifier).add_bracket_index(index)
            else:
                return ColumnBuilder(identifier)

        return self._parse_column_identifier(name, tree)

    def _parse_column_identifier(
        self, name: str, tree: sqlglot.Expression
    ) -> ColumnBuilder:
        # Columns which contains spaces will be registered as aliases.
        # Check that no quotes have been applied to either end of the name:
        # - "sur" name, sur "name" are both invalid column names
        if tree.find(exp.Alias):
            if any([identifier.quoted for identifier in tree.find_all(exp.Identifier)]):
                raise ParseError(
                    f"The supplied column name '{name}' contains quotes and cannot be "
                    "parsed as a valid SQL identifier."
                )

        if tree.find(exp.Bracket):
            # Pop the column, leaving the bracket as the remaining section of the tree
            column_tree = tree.find(exp.Column).pop()
            return ColumnBuilder.with_sqlglot_column(column_tree, bracket_index=tree)

        # If the column has already been quoted, then parse it
        if tree.find(exp.Identifier).args.get("quoted"):
            return ColumnBuilder.with_sqlglot_column(tree)

        # If our column does not contain a quoted identifier, we can safely return
        # the column builder
        return ColumnBuilder(name)

    @staticmethod
    def manually_split_identifier_and_index(identifier: str):
        # Manually pull out the bracket index for an unparseable column.
        # This ensures we correctly parse cases such as: sur name['lat']
        if identifier.endswith("]") and "[" in identifier:
            split_index = identifier.rfind("[")
            before_bracket = identifier[:split_index].strip()
            after_bracket = identifier[split_index:].strip("[]'")
            return before_bracket, after_bracket
        else:
            return identifier.strip("[],"), None

    @property
    def _bf_prefix(self):
        return self.from_settings_obj_else_default(
            "_bf_prefix", "bayes_factor_column_prefix"
        )

    @property
    def _tf_prefix(self):
        return self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

    def _copy_with_new_column_builder(self, new_builder) -> InputColumn:
        input_column_copy = deepcopy(self)
        input_column_copy.column_builder = new_builder
        return input_column_copy

    def change_input_column_names(self, new_name: str):
        return self._copy_with_new_column_builder(
            self.column_builder.change_name(new_name)
        )

    def unquote(self) -> InputColumn:
        return self._copy_with_new_column_builder(self.column_builder.unquote())

    def exclude_expressions(self) -> InputColumn:
        return self._copy_with_new_column_builder(
            self.column_builder.exclude_expressions()
        )

    def add_bracket_index_to_col(self, index_name: str):
        """
        Manually add a bracket index onto your column - ['idx'].

        This allows you to use illegal name and bracket combinations such as
        'SUR NAME['lat']', without needing to add quotes to the column identifier.
        """
        self.column_builder = self.column_builder.add_bracket_index(index_name)

    @property
    def as_base_dialect(self) -> InputColumn:
        input_column_copy = deepcopy(self)
        input_column_copy.sqlglot_name = None
        return input_column_copy

    def column_tree_to_sql(self, column_tree: ColumnBuilder) -> str:
        return column_tree.build_column_tree_with_expressions().sql(self.sqlglot_name)

    def _table_name_as(self, table: str, prefix: str = "", suffix: str = "") -> str:
        # Remove quotes and expressions, if applied. This prevents
        # the accidental addition of quotes to the alias.
        # See: exp.Identifier(this='`test`', quoted=True).sql("spark")
        column_builder = self.column_builder.exclude_expressions().unquote()
        # Add both the prefix and suffix arguments to the column
        alias_tree = column_builder.add_prefix(prefix).add_suffix(suffix)
        alias_string = self.column_tree_to_sql(alias_tree)

        column_with_table = self.column_builder.add_table(table).add_prefix(prefix)
        tree = column_with_table.add_alias(alias_string)
        return tree.sql(self.sqlglot_name)

    @property
    def name(self):
        tree = self.column_builder
        return self.column_tree_to_sql(tree)

    @property
    def name_l(self):
        tree = self.column_builder.add_suffix("_l")
        return self.column_tree_to_sql(tree)

    @property
    def name_r(self):
        tree = self.column_builder.add_suffix("_r")
        return self.column_tree_to_sql(tree)

    @property
    def names_l_r(self):
        return [self.name_l, self.name_r]

    @property
    def l_name_as_l(self) -> str:
        return self._table_name_as(table="l", suffix="_l")

    @property
    def r_name_as_r(self) -> str:
        return self._table_name_as(table="r", suffix="_r")

    @property
    def l_r_names_as_l_r(self) -> list[str]:
        return [self.l_name_as_l, self.r_name_as_r]

    @property
    def bf_name(self) -> str:
        tree = self.column_builder.add_prefix(self._bf_prefix)
        return self.column_tree_to_sql(tree)

    @property
    def tf_name(self) -> str:
        tree = self.column_builder.add_prefix(self._tf_prefix)
        return self.column_tree_to_sql(tree)

    @property
    def tf_name_l(self) -> str:
        tree = self.column_builder.add_prefix(self._tf_prefix).add_suffix("_l")
        return self.column_tree_to_sql(tree)

    @property
    def tf_name_r(self) -> str:
        tree = self.column_builder.add_prefix(self._tf_prefix).add_suffix("_r")
        return self.column_tree_to_sql(tree)

    @property
    def tf_name_l_r(self) -> list[str]:
        return [self.tf_name_l, self.tf_name_r]

    @property
    def l_tf_name_as_l(self) -> str:
        return self._table_name_as(table="l", prefix=self._tf_prefix, suffix="_l")

    @property
    def r_tf_name_as_r(self) -> str:
        return self._table_name_as(table="r", prefix=self._tf_prefix, suffix="_r")

    @property
    def l_r_tf_names_as_l_r(self) -> list[str]:
        return [self.l_tf_name_as_l, self.r_tf_name_as_r]

    def lowercase_column(self, to_lowercase: bool) -> InputColumn:
        if to_lowercase:
            lowercase_sql = sqlglot.parse_one("lower(col)", self.sqlglot_name)
            self.column_builder.append_expression(lowercase_sql)
        return self

    @unsupported_splink_dialect_expression(["postgres", "sqlite"])
    def regex_extract(self, regex: str) -> InputColumn:
        if regex:
            regex_sql = f"regexp_extract(col, '{regex}', 0)"
            self.column_builder.append_expression(
                sqlglot.parse_one(regex_sql, self.sqlglot_name)
            )
        return self

    @unsupported_splink_dialect_expression([])  # check if a dialect has been supplied
    def str_to_date(self, cast_dates_to_string: bool, date_format: str):
        if cast_dates_to_string:
            str_to_date_function = self._sql_dialect.str_to_date
            str_to_date_sql = f"{str_to_date_function}(col, '{date_format}')"

            self.column_builder.append_expression(
                sqlglot.parse_one(str_to_date_sql, self.sqlglot_name)
            )
        return self

    @property
    def reset_expressions(self):
        """Reset the column expressions within the column builder."""
        self.column_builder.column_expressions = []

    def __repr__(self):
        return f"InputColumn({self.column_builder.__repr__()})"


def _get_dialect_quotes(dialect):
    """
    Returns the appropriate quotation marks for identifiers based on the SQL dialect.

    For most SQL dialects, identifiers are quoted using double quotes.
    For example, "first name" is a quoted identifier that
    allows for a space in the column name.

    However, some SQL dialects, use other identifiers e.g. ` in Spark SQL
    """
    start = end = '"'
    if dialect is None:
        return start, end
    try:
        sqlglot_dialect = sqlglot.Dialect[dialect.lower()]
    except KeyError:
        return start, end
    return _get_sqlglot_dialect_quotes(sqlglot_dialect)


def _get_sqlglot_dialect_quotes(dialect: sqlglot.Dialect):
    try:
        # For sqlglot >= 16.0.0
        start = dialect.IDENTIFIER_START
        end = dialect.IDENTIFIER_END
    except AttributeError:
        start = dialect.identifier_start
        end = dialect.identifier_end
    return start, end

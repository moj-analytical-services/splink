from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace, field

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.expressions import Expression

from .default_from_jsonschema import default_value_from_schema


@dataclass(frozen=True)
class ColumnTreeBuilder:
    """
    A class that encapsulates the column name (e.g. first_name or first name),
    or column reference (e.g. coords["lat"] or coords[0]) that represents an
    input column into Splink.

    The class facilitates common manipulations of the syntax tree such as
    adding prefixes, suffixes, an associated table, identifiers, quotes etc.

    The class limits its concerns to modifying the column name or reference itself,
    as opposed to further generic manipulations such as wrapping with LOWER() etc.

    All methods produce a copy of the object to prevent inadvertent
    modification of the original object.

    The build_column_tree method returns a modified column name or
    a column reference as a sqlglot expression tree.
    """

    name: str
    table: str = field(default=None, repr=False)
    quoted: bool = True
    # key_or_field is the "lat" part of coords["lat"] or the 1 parts of coords[1]
    key_or_field: exp.Identifier = None

    def unquote(self):
        return replace(self, quoted=False)

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
        return self.as_sqlglot_expression_tree().as_(alias)

    def add_column_key_or_field(self, index_name: str | int):
        """
        Manually add a column reference to your column - ['idx'], for example.

        This is useful during the parsing process or when you try to combine
        illegal name and bracket combinations such as 'SUR NAME['lat']',
        without needing to add quotes to the column identifier.
        """
        is_str = type(index_name) == str
        self.key_or_field = exp.Literal(this=index_name, is_string=is_str)
        return self

    def as_sqlglot_expression_tree(self):
        # Create our column representation
        column = sqlglot.column(col=self.name, table=self.table, quoted=self.quoted)
        # Column is made up of the identifier + bracket index (if it exists)
        if self.key_or_field:
            return exp.Bracket(this=column, expressions=[self.key_or_field])
        else:
            return column

    @classmethod
    def from_raw_column_name_or_column_reference(
        cls,
        name: str,
        sqlglot_name: str
    ) -> ColumnTreeBuilder:
        """
        Parses a given input name into a ColumnTreeBuilder.

        Fiddly because we need to deal with escaping issues. For example
        the column name in the input dataset may be 'first and surname', but
        if we naively parse this using sqlglot it will be interpreted as an AND
        expression
        """
        try:
            tree = sqlglot.parse_one(name, read=sqlglot_name)
            return cls._parse_sql_tree_to_column_tree_builder(name, tree)
        except:
            q_s, q_e = _get_dialect_quotes(sqlglot_name)
            # The parse statement will error in cases such as: sur "name"['lat']
            sqlglot.parse_one(f"{q_s}{name}{q_e}", read=sqlglot_name)
            # index is None if nothing is set
            identifier, index = cls.manually_split_identifier_and_index(name)

            # Construct the column manually if it can be parsed with quotes.
            # This is useful for illegal columns such as test[ing, test'ing, test"ing
            return cls(name=identifier, key_or_field=index)

    @classmethod
    def _parse_sql_tree_to_column_tree_builder(
        cls, name: str, tree: sqlglot.Expression
    ) -> ColumnTreeBuilder:
        # Columns which contains spaces will be registered as aliases.
        # Check that no quotes have been applied to either end of the name:
        # - "sur" name, sur "name" are both invalid column names
        if tree.find(exp.Alias):
            if any([identifier.quoted for identifier in tree.find_all(exp.Identifier)]):
                raise ParseError(
                    f"The supplied column name '{name}' contains quotes and cannot be "
                    "parsed as a valid SQL identifier."
                )

        # If the column has an identifier, parse both elements
        if tree.find(exp.Bracket):
            # Note: Sqlglot parses the key_or_field attribute of an identifier -
            # 'lat' from coords["lat"] -  to the 'expressions' attribute on
            # the Bracket expression:
            # tree = sqlglot.parse_one('select coords["lat"]')
            # tree.find(sqlglot.expressions.Bracket).expressions
            # > [(LITERAL this: lat, is_string: True)]
            return cls(
                name=tree.find(exp.Column).name,
                key_or_field=tree.find(exp.Literal),
            )

        # If the column has already been quoted, parse it
        if tree.find(exp.Identifier).args.get("quoted"):
            return cls(name=tree.name)

        # If our column does not contain a quoted identifier, we can safely return
        # the column builder
        return cls(name)

    @staticmethod
    def manually_split_identifier_and_index(identifier: str) -> [str, str]:
        # Manually pull out the bracket index for an unparseable column.
        # This ensures we correctly parse cases such as: sur name['lat']
        if identifier.endswith("]") and "[" in identifier:
            split_index = identifier.rfind("[")
            before_bracket = identifier[:split_index].strip()
            after_bracket = identifier[split_index:].strip("[]")
            return before_bracket, after_bracket
        else:
            return identifier, None


class InputColumn:
    """
    A wrapper class that simplifies interactions with a ColumnTreeBuilder instance.

    This class serves as a simpler interface to the ColumnTreeBuilder, which is responsible
    for building and returning a SQLglot expression tree representing SQL column references.
    The InputColumn class provides user-friendly helper methods for commonly required
    column manipulations and transformations.

    Methods such as '.name_l' run the relevant of building steps on the tree
    builder and convert the resulting tree into a valid SQL column reference. These methods
    make it easier to perform operations like quoting, adding prefixes/suffixes, and
    formatting column references according to specific SQL dialects.

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
        sql_dialect: str = None,
    ):
        # If settings_obj is None, then default values will be used
        # from the jsonschama
        self._settings_obj = settings_obj

        self.register_dialect(sql_dialect)

        self.input_name: str = self._quote_if_sql_keyword(
            raw_column_name_or_column_reference
        )

        # Generate a ColumnTreeBuilder from the input column
        tree_builder = ColumnTreeBuilder.from_raw_column_name_or_column_reference(
                name=raw_column_name_or_column_reference,
                sqlglot_name=sql_dialect
            )
        self.base_column_tree: ColumnTreeBuilder = tree_builder

    def register_dialect(self, sql_dialect: str):
        if not sql_dialect and self._settings_obj:
            sql_dialect = self._settings_obj._sql_dialect

        self.sqlglot_name = sql_dialect

    def from_settings_obj_else_default(self, key, schema_key=None):
        # Covers the case where no settings obj is set on the comparison level
        if self._settings_obj:
            return getattr(self._settings_obj, key)
        else:
            if not schema_key:
                schema_key = key
            return default_value_from_schema(schema_key, "root")

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
        input_column_copy.base_column_tree = new_builder
        return input_column_copy

    def unquote(self) -> InputColumn:
        return self._copy_with_new_column_builder(self.base_column_tree.unquote())

    @property
    def as_base_dialect(self) -> InputColumn:
        input_column_copy = deepcopy(self)
        input_column_copy.sqlglot_name = None
        return input_column_copy

    def column_tree_to_sql(self, column_tree: ColumnTreeBuilder) -> str:
        return column_tree.as_sqlglot_expression_tree().sql(self.sqlglot_name)

    def _table_name_as(self, table: str, prefix: str = "", suffix: str = "") -> str:
        # Remove quotes, if applied. This prevents
        # the accidental addition of quotes to the alias.
        # See: exp.Identifier(this='`test`', quoted=True).sql("spark")
        column_builder = self.base_column_tree.unquote()
        # Add both the prefix and suffix arguments to the column
        alias_tree = column_builder.add_prefix(prefix).add_suffix(suffix)
        alias_string = self.column_tree_to_sql(alias_tree)

        column_with_table = self.base_column_tree.add_table(table).add_prefix(prefix)
        tree = column_with_table.add_alias(alias_string)
        return tree.sql(self.sqlglot_name)

    @property
    def name(self):
        tree = self.base_column_tree
        return self.column_tree_to_sql(tree)

    @property
    def name_l(self):
        tree = self.base_column_tree.add_suffix("_l")
        return self.column_tree_to_sql(tree)

    @property
    def name_r(self):
        tree = self.base_column_tree.add_suffix("_r")
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
        tree = self.base_column_tree.add_prefix(self._bf_prefix)
        return self.column_tree_to_sql(tree)

    @property
    def tf_name(self) -> str:
        tree = self.base_column_tree.add_prefix(self._tf_prefix)
        return self.column_tree_to_sql(tree)

    @property
    def tf_name_l(self) -> str:
        tree = self.base_column_tree.add_prefix(self._tf_prefix).add_suffix("_l")
        return self.column_tree_to_sql(tree)

    @property
    def tf_name_r(self) -> str:
        tree = self.base_column_tree.add_prefix(self._tf_prefix).add_suffix("_r")
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

    def _quote_if_sql_keyword(self, name: str) -> str:
        if name not in {"group", "index"}:
            return name
        start, end = _get_dialect_quotes(self.sqlglot_name)
        return start + name + end

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(\n     "
            f"{self.base_column_tree.__repr__()},\n     "
            f"sql_dialect={self.sqlglot_name}\n)"
        )


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

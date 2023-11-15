from __future__ import annotations

from copy import deepcopy

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from sqlglot.expressions import Expression

from .default_from_jsonschema import default_value_from_schema


def sqlglot_tree_signature(tree):
    """
    A short string representation of a SQLglot tree.

    Allows you to easily check that a tree contains certain nodes

    For instance, the string "robin['hi']" becomes:
    'bracket column literal identifier'
    """
    return " ".join(n[0].key for n in tree.walk())


def add_suffix(tree, suffix) -> Expression:
    tree = tree.copy()
    identifier_string = tree.find(exp.Identifier).this
    identifier_string = f"{identifier_string}{suffix}"
    tree.find(exp.Identifier).args["this"] = identifier_string
    return tree


def add_prefix(tree, prefix) -> Expression:
    tree = tree.copy()
    identifier_string = tree.find(exp.Identifier).this
    identifier_string = f"{prefix}{identifier_string}"
    tree.find(exp.Identifier).args["this"] = identifier_string
    return tree


def add_table(tree, tablename) -> Expression:
    tree = tree.copy()
    table_identifier = exp.Identifier(this=tablename, quoted=True)
    identifier = tree.find(exp.Column)
    identifier.args["table"] = table_identifier
    return tree


def remove_quotes_from_identifiers(tree) -> Expression:
    tree = tree.copy()
    for identifier in tree.find_all(exp.Identifier):
        identifier.args["quoted"] = False
    return tree


class InputColumn:
    """
    Represents a SQL column or column reference
    Handles SQL dialect-specific issues such as identifier quoting.

    The input should be the raw identifier, without SQL-specific identifier quotes.

    For example, if a column is named 'first name' (with a space), the input should be
    'first name', not '"first name"'.

    Examples of valid inputs include:
    - 'first_name'
    - 'first name'
    - 'coordinates['lat']'
    - 'coordinates[1]'

    """

    def __init__(
        self, raw_column_name_or_column_reference, settings_obj=None, sql_dialect=None
    ):
        # If settings_obj is None, then default values will be used
        # from the jsonschama
        self._settings_obj = settings_obj

        if sql_dialect:
            self._sql_dialect = sql_dialect
        elif settings_obj:
            self._sql_dialect = self._settings_obj._sql_dialect
        else:
            self._sql_dialect = None

        self.input_name = self._quote_if_sql_keyword(
            raw_column_name_or_column_reference
        )

        self.input_name_as_tree = self.parse_input_name_to_sqlglot_tree()

        for identifier in self.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = True

    def quote(self) -> "InputColumn":
        self_copy = deepcopy(self)
        for identifier in self_copy.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = True
        return self_copy

    def unquote(self) -> "InputColumn":
        self_copy = deepcopy(self)
        for identifier in self_copy.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = False
        return self_copy

    def parse_input_name_to_sqlglot_tree(self) -> Expression:
        """
        Parses the input name into a SQLglot expression tree.

        Fiddly because we need to deal with escaping issues.  For example
        the column name in the input dataset may be 'first and surname', but
        if we naively parse this using sqlglot it will be interpreted as an AND
        expression

        Note: We do not support inputs like 'SUR name[1]', in this case the user
        would have to quote e.g. `SUR name`[1]
        """

        q_s, q_e = _get_dialect_quotes(self._sql_dialect)

        try:
            tree = sqlglot.parse_one(self.input_name, read=self._sql_dialect)
        except ParseError:
            tree = sqlglot.parse_one(
                f"{q_s}{self.input_name}{q_e}", read=self._sql_dialect
            )

        tree_signature = sqlglot_tree_signature(tree)
        valid_signatures = ["column identifier", "bracket column literal identifier"]

        if tree_signature in valid_signatures:
            return tree
        else:
            # e.g. SUR name parses to 'alias column identifier identifier'
            # but we want "SUR name"
            tree = sqlglot.parse_one(
                f"{q_s}{self.input_name}{q_e}", read=self._sql_dialect
            )
            return tree

    def from_settings_obj_else_default(self, key, schema_key=None):
        # Covers the case where no settings obj is set on the comparison level
        if self._settings_obj:
            return getattr(self._settings_obj, key)
        else:
            if not schema_key:
                schema_key = key
            return default_value_from_schema(schema_key, "root")

    @property
    def gamma_prefix(self) -> str:
        return self.from_settings_obj_else_default(
            "_gamma_prefix", "comparison_vector_value_column_prefix"
        )

    @property
    def bf_prefix(self) -> str:
        return self.from_settings_obj_else_default(
            "_bf_prefix", "bayes_factor_column_prefix"
        )

    @property
    def tf_prefix(self) -> str:
        return self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

    @property
    def name(self) -> str:
        return self.input_name_as_tree.sql(dialect=self._sql_dialect)

    @property
    def name_l(self) -> str:
        return add_suffix(self.input_name_as_tree, suffix="_l").sql(
            dialect=self._sql_dialect
        )

    @property
    def name_r(self) -> str:
        return add_suffix(self.input_name_as_tree, suffix="_r").sql(
            dialect=self._sql_dialect
        )

    @property
    def names_l_r(self) -> list[str]:
        return [self.name_l, self.name_r]

    @property
    def l_name_as_l(self) -> str:
        name_with_l_table = add_table(self.input_name_as_tree, "l").sql(
            dialect=self._sql_dialect
        )
        return f"{name_with_l_table} as {self.name_l}"

    @property
    def r_name_as_r(self) -> str:
        name_with_r_table = add_table(self.input_name_as_tree, "r").sql(
            dialect=self._sql_dialect
        )
        return f"{name_with_r_table} as {self.name_r}"

    @property
    def l_r_names_as_l_r(self) -> list[str]:
        return [self.l_name_as_l, self.r_name_as_r]

    @property
    def bf_name(self) -> str:
        return add_prefix(self.input_name_as_tree, prefix=self.bf_prefix).sql(
            dialect=self._sql_dialect
        )

    @property
    def tf_name(self) -> str:
        return add_prefix(self.input_name_as_tree, prefix=self.tf_prefix).sql(
            dialect=self._sql_dialect
        )

    @property
    def tf_name_l(self) -> str:
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        return add_suffix(tree, suffix="_l").sql(dialect=self._sql_dialect)

    @property
    def tf_name_r(self) -> str:
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        return add_suffix(tree, suffix="_r").sql(dialect=self._sql_dialect)

    @property
    def tf_name_l_r(self) -> list[str]:
        return [self.tf_name_l, self.tf_name_r]

    @property
    def l_tf_name_as_l(self) -> str:
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        tf_name_with_l_table = add_table(tree, tablename="l").sql(
            dialect=self._sql_dialect
        )
        return f"{tf_name_with_l_table} as {self.tf_name_l}"

    @property
    def r_tf_name_as_r(self) -> str:
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        tf_name_with_r_table = add_table(tree, tablename="r").sql(
            dialect=self._sql_dialect
        )
        return f"{tf_name_with_r_table} as {self.tf_name_r}"

    @property
    def l_r_tf_names_as_l_r(self) -> list[str]:
        return [self.l_tf_name_as_l, self.r_tf_name_as_r]

    def _quote_if_sql_keyword(self, name: str) -> str:
        if name not in {"group", "index"}:
            return name
        start, end = _get_dialect_quotes(self._sql_dialect)
        return start + name + end


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

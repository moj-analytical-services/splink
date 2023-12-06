from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace

import sqlglot
import sqlglot.expressions as exp

from .default_from_jsonschema import default_value_from_schema
from .sql_transform import sqlglot_tree_signature


@dataclass(frozen=True)
class SqlglotColumnTreeBuilder:
    """
    Builds a sqlglot expression tree representing a column or column reference
    from its arguments.

    Since this is a frozen dataclass, it's easy to modify the column or column
    reference using the `replace` method.

    For instance, to add a `_l` to column_name, you can do:

        new_column_name = col_builder.column_name + "_l"
        replace(col_builder, column_name=new_column_name).sql


    The `sql` property returns the sql string corresopnding to the tree
    """

    column_name: str
    table: str = None
    quoted: bool = True
    bracket_index: int = None
    bracket_key: str = None
    sqlglot_dialect: str = None
    alias: str = None

    @property
    def _has_key_or_index(self):
        return self.bracket_index is not None or self.bracket_key is not None

    def _add_key_or_index_to_tree(self, tree):
        if self.bracket_key is not None:
            is_string = True
            literal = self.bracket_key
        elif self.bracket_index is not None:
            is_string = False
            literal = f"{self.bracket_index}"

        tree = exp.Bracket(
            this=tree,
            expressions=[exp.Literal(this=literal, is_string=is_string)],
        )
        return tree

    def _wrap_if_has_alias(self, tree):
        if self.alias is None:
            return tree
        else:
            return exp.alias_(tree, self.alias)

    @property
    def as_sqlglot_tree(self):
        tree = sqlglot.column(
            col=self.column_name, table=self.table, quoted=self.quoted
        )
        if self._has_key_or_index:
            tree = self._add_key_or_index_to_tree(tree)
        if self.alias:
            return exp.alias_(tree, self.alias, quoted=self.quoted)
        return tree

    @property
    def sql(self):
        return self.as_sqlglot_tree.sql(dialect=self.sqlglot_dialect)

    @classmethod
    def from_raw_column_name_or_column_reference(cls, input_str, sqlglot_dialect):
        def tree_to_sqlglot_column_tree_builder_args(sqlglot_tree, sqlglot_dialect):
            args = {"sqlglot_dialect": sqlglot_dialect, "quoted": True}
            if sqlglot_tree.find(exp.Bracket):
                lit = sqlglot_tree.find(exp.Bracket).find(exp.Literal)
                if lit.args["is_string"]:
                    args["bracket_key"] = lit.args["this"]
                else:
                    args["bracket_index"] = int(lit.args["this"])

            args["column_name"] = sqlglot_tree.find(exp.Identifier).args["this"]
            return args

        def add_quotes_to_column_name(input_str, q_s, q_e):
            if input_str.rfind("[") != -1 and input_str.endswith("]"):
                index = input_str.rfind("[")
                name = input_str[:index]
                key_or_index = input_str[index:]
                return f"{q_s}{name}{q_e}{key_or_index}"
            else:
                return f"{q_s}{input_str}{q_e}"

        valid_signatures = {
            sqlglot_tree_signature(sqlglot.parse_one("col_name")),
            sqlglot_tree_signature(sqlglot.parse_one("col_name[1]")),
            sqlglot_tree_signature(sqlglot.parse_one("col_name['lat']")),
        }

        # If the raw string parses to a valid signature, use it
        try:
            tree = sqlglot.parse_one(input_str, dialect=sqlglot_dialect)
        except (sqlglot.ParseError, sqlglot.TokenError):
            pass
        else:
            sig = sqlglot_tree_signature(tree)
            if sig in valid_signatures:
                args = tree_to_sqlglot_column_tree_builder_args(tree, sqlglot_dialect)
                return cls(**args)

        # If not, it's probably an escaping issue.  We don't require that the input is
        # properly escaped using identifier quotes so e.g. if there is a space in the
        # input_str, it will be incorrectly parsed.
        # Possible cases are: first name, lat long[1] or lat long['lat']
        # The space could also be any arbitrary character e.g. first[name
        q_s, q_e = _get_dialect_quotes(sqlglot_dialect)
        input_str = add_quotes_to_column_name(input_str, q_s, q_e)
        try:
            tree = sqlglot.parse_one(input_str, dialect=sqlglot_dialect)
        except (sqlglot.ParseError, sqlglot.TokenError):
            pass
        else:
            sig = sqlglot_tree_signature(tree)
            if sig in valid_signatures:
                args = tree_to_sqlglot_column_tree_builder_args(tree, sqlglot_dialect)
                return cls(**args)

        raise ValueError(f"Could not parse input column: {input_str}")


class InputColumn:
    """
    Represents a column or column reference in the input data to Splink.

    Handles identifier quotes for the user, so the user can e.g. provide column names
    like "first name" instead of having to use '"first name"'.  The rationale is
    that:
    -  many users won't understand the difference between ` ' and " in SQL and are
    unlikely to provide correct identifier quotes
    - it's inconvenient and fiddly in Python to provide identifier quotes in a string

    Handles the various transformations needed by Splink such as adding `_l` and `_r`,
    table names etc.

    Uses `SqlglotColumnTreeBuilder` to manipulate the sqlglot expression tree
    representing the column or column reference

    The input can be either the raw identifier, or an identifier with
    SQL-specific identifier quotes.

    Examples of valid inputs include:
    - 'first_name' (column name with no identifier quotes)
    - 'first[name'  (column name with a special character)
    - '"first name"' (column name with identifier quotes)
    - 'coordinates['lat']' (Column name for a struct column)
    - '"sur NAME"['lat'] (Column name with identifier quotes for a struct column)
    - 'coordinates[1]' (Column name for an array column)
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

        # Handle the case that the column name is a sql keyword like 'group'
        self.input_name: str = self._quote_if_sql_keyword(
            raw_column_name_or_column_reference
        )

        self.col_builder: SqlglotColumnTreeBuilder = (
            SqlglotColumnTreeBuilder.from_raw_column_name_or_column_reference(
                raw_column_name_or_column_reference,
                sqlglot_dialect=self.sql_dialect,
            )
        )

    def register_dialect(self, sql_dialect: str):
        if not sql_dialect and self._settings_obj:
            sql_dialect = self._settings_obj._sql_dialect

        self.sql_dialect = sql_dialect

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

    def unquote(self) -> InputColumn:
        self_copy = deepcopy(self)
        b = replace(self_copy.col_builder, quoted=False)
        self_copy.col_builder = b
        return self_copy

    def quote(self) -> InputColumn:
        self_copy = deepcopy(self)
        b = replace(self_copy.col_builder, quoted=True)
        self_copy.col_builder = b
        return self_copy

    @property
    def as_base_dialect(self) -> InputColumn:
        input_column_copy = deepcopy(self)
        input_column_copy.sql_dialect = None
        return input_column_copy

    @property
    def name(self):
        return self.col_builder.sql

    @property
    def name_l(self):
        new_column_name = self.col_builder.column_name + "_l"
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def name_r(self):
        new_column_name = self.col_builder.column_name + "_r"
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def names_l_r(self):
        return [self.name_l, self.name_r]

    @property
    def l_name_as_l(self) -> str:
        alias = self.unquote().name_l
        return replace(self.col_builder, table="l", alias=alias).sql

    @property
    def r_name_as_r(self) -> str:
        alias = self.unquote().name_r
        return replace(self.col_builder, table="r", alias=alias).sql

    @property
    def l_r_names_as_l_r(self) -> list[str]:
        return [self.l_name_as_l, self.r_name_as_r]

    @property
    def bf_name(self) -> str:
        new_column_name = self._bf_prefix + self.col_builder.column_name
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def tf_name(self) -> str:
        new_column_name = self._tf_prefix + self.col_builder.column_name
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def tf_name_l(self) -> str:
        new_column_name = self._tf_prefix + self.col_builder.column_name + "_l"
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def tf_name_r(self) -> str:
        new_column_name = self._tf_prefix + self.col_builder.column_name + "_r"
        return replace(self.col_builder, column_name=new_column_name).sql

    @property
    def tf_name_l_r(self) -> list[str]:
        return [self.tf_name_l, self.tf_name_r]

    @property
    def l_tf_name_as_l(self) -> str:
        alias = self._tf_prefix + self.unquote().name_l
        name = self._tf_prefix + self.col_builder.column_name
        return replace(self.col_builder, table="l", column_name=name, alias=alias).sql

    @property
    def r_tf_name_as_r(self) -> str:
        alias = self._tf_prefix + self.unquote().name_r
        name = self._tf_prefix + self.col_builder.column_name
        return replace(self.col_builder, table="r", column_name=name, alias=alias).sql

    @property
    def l_r_tf_names_as_l_r(self) -> list[str]:
        return [self.l_tf_name_as_l, self.r_tf_name_as_r]

    def _quote_if_sql_keyword(self, name: str) -> str:
        if name not in {"group", "index"}:
            return name
        start, end = _get_dialect_quotes(self.sql_dialect)
        return start + name + end

    def __repr__(self):
        return f"{self.__class__.__name__}\n({self.col_builder.__repr__()}\n)"


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

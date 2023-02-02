import sqlglot
import sqlglot.expressions as exp
from copy import deepcopy

from sqlglot.errors import ParseError

from .default_from_jsonschema import default_value_from_schema


def sqlglot_tree_signature(tree):
    """
    A short string representation of a SQLglot tree.

    Allows you to easily check that a tree contains certain nodes

    For instance, the string "robin['hi']" becomes:
    'bracket column literal identifier'
    """
    return " ".join(n[0].key for n in tree.walk())


def add_suffix(tree, suffix):
    tree = tree.copy()
    identifier_string = tree.find(exp.Identifier).this
    identifier_string = f"{identifier_string}{suffix}"
    tree.find(exp.Identifier).args["this"] = identifier_string
    return tree


def add_prefix(tree, prefix):
    tree = tree.copy()
    identifier_string = tree.find(exp.Identifier).this
    identifier_string = f"{prefix}{identifier_string}"
    tree.find(exp.Identifier).args["this"] = identifier_string
    return tree


def add_table(tree, tablename):
    tree = tree.copy()
    table_identifier = exp.Identifier(this=tablename, quoted=True)
    identifier = tree.find(exp.Column)
    identifier.args["table"] = table_identifier
    return tree


def remove_quotes_from_identifiers(tree):
    tree = tree.copy()
    for identifier in tree.find_all(exp.Identifier):
        identifier.args["quoted"] = False
    return tree


class InputColumn:
    def __init__(self, name, settings_obj=None, sql_dialect=None):

        # If settings_obj is None, then default values will be used
        # from the jsonschama
        self._settings_obj = settings_obj

        if sql_dialect:
            self._sql_dialect = sql_dialect
        elif settings_obj:
            self._sql_dialect = getattr(self._settings_obj, "_sql_dialect")
        else:
            self._sql_dialect = None

        valid_dialects = [name.lower() for name in sqlglot.Dialects._member_names_]
        if self._sql_dialect in valid_dialects:
            quote_characters = sqlglot.Dialect[self._sql_dialect.lower()].identifiers
            quote_character = '"' if '"' in quote_characters else quote_characters[0]
        else:
            quote_character = '"'

        # Quote SQL keywords
        if name in ("group", "index"):
            name = f"{quote_character}{name}{quote_character}"

        self.input_name = name

        self.input_name_as_tree = self.parse_input_name_to_sqlglot_tree()

        for identifier in self.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = True

    def quote(self):
        self_copy = deepcopy(self)
        for identifier in self_copy.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = True
        return self_copy

    def unquote(self):
        self_copy = deepcopy(self)
        for identifier in self_copy.input_name_as_tree.find_all(exp.Identifier):
            identifier.args["quoted"] = False
        return self_copy

    def parse_input_name_to_sqlglot_tree(self):

        # Cases that could occur for self.input_name:
        # SUR name  -> parses to 'alias column identifier identifier'
        # first and surname -> parses to 'and column column identifier identifier'
        # a b c -> parse error
        # "SUR name" -> parses to 'column identifier'
        # geocode['lat'] -> parsees to bracket column literal identifier
        # geocode[1] -> parsees to bracket column literal identifier

        # Note we don't expect SUR name[1] since the user should have quoted this

        try:
            tree = sqlglot.parse_one(self.input_name, read=self._sql_dialect)
        except ParseError:
            tree = sqlglot.parse_one(f'"{self.input_name}"', read=self._sql_dialect)

        tree_signature = sqlglot_tree_signature(tree)
        valid_signatures = ["column identifier", "bracket column literal identifier"]

        if tree_signature in valid_signatures:
            return tree
        else:
            # e.g. SUR name parses to 'alias column identifier identifier'
            # but we want "SUR name"
            tree = sqlglot.parse_one(f'"{self.input_name}"', read=self._sql_dialect)
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
    def gamma_prefix(self):
        return self.from_settings_obj_else_default(
            "_gamma_prefix", "comparison_vector_value_column_prefix"
        )

    @property
    def bf_prefix(self):
        return self.from_settings_obj_else_default(
            "_bf_prefix", "bayes_factor_column_prefix"
        )

    @property
    def tf_prefix(self):
        return self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

    def name(self):
        return self.input_name_as_tree.sql(dialect=self._sql_dialect)

    def name_l(self):
        return add_suffix(self.input_name_as_tree, suffix="_l").sql(
            dialect=self._sql_dialect
        )

    def name_r(self):
        return add_suffix(self.input_name_as_tree, suffix="_r").sql(
            dialect=self._sql_dialect
        )

    def names_l_r(self):
        return [self.name_l(), self.name_r()]

    def l_name_as_l(self):
        name_with_l_table = add_table(self.input_name_as_tree, "l").sql(
            dialect=self._sql_dialect
        )
        return f"{name_with_l_table} as {self.name_l()}"

    def r_name_as_r(self):
        name_with_r_table = add_table(self.input_name_as_tree, "r").sql(
            dialect=self._sql_dialect
        )
        return f"{name_with_r_table} as {self.name_r()}"

    def l_r_names_as_l_r(self):
        return [self.l_name_as_l(), self.r_name_as_r()]

    def bf_name(self):
        return add_prefix(self.input_name_as_tree, prefix=self.bf_prefix).sql(
            dialect=self._sql_dialect
        )

    def tf_name(self):
        return add_prefix(self.input_name_as_tree, prefix=self.tf_prefix).sql(
            dialect=self._sql_dialect
        )

    def tf_name_l(self):
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        return add_suffix(tree, suffix="_l").sql(dialect=self._sql_dialect)

    def tf_name_r(self):
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        return add_suffix(tree, suffix="_r").sql(dialect=self._sql_dialect)

    def tf_name_l_r(self):
        return [self.tf_name_l(), self.tf_name_r()]

    def l_tf_name_as_l(self):
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        tf_name_with_l_table = add_table(tree, tablename="l").sql(
            dialect=self._sql_dialect
        )
        return f"{tf_name_with_l_table} as {self.tf_name_l()}"

    def r_tf_name_as_r(self):
        tree = add_prefix(self.input_name_as_tree, prefix=self.tf_prefix)
        tf_name_with_r_table = add_table(tree, tablename="r").sql(
            dialect=self._sql_dialect
        )
        return f"{tf_name_with_r_table} as {self.tf_name_r()}"

    def l_r_tf_names_as_l_r(self):
        return [self.l_tf_name_as_l(), self.r_tf_name_as_r()]

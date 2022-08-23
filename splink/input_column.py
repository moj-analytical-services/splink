import sqlglot
import sqlglot.expressions as exp

from sqlglot.errors import ParseError

from .default_from_jsonschema import default_value_from_schema
from splink.sql_transform import add_prefix_or_suffix_to_colname


def sqlglot_tree_signature(tree):
    """
    A short string representation of a SQLglot tree.

    Allows you to easily check that a tree contains certain nodes

    For instance, the string "robin['hi']" becomes:
    'bracket column literal identifier'
    """
    return " ".join(n[0].key for n in tree.walk())


class InputColumn:
    def __init__(self, name, tf_adjustments=False, settings_obj=None, sql_dialect=None):

        # If settings_obj is None, then default values will be used
        # from the jsonschama
        self._settings_obj = settings_obj
        self._has_tf_adjustments = tf_adjustments

        self.input_name = name

        if sql_dialect:
            self._sql_dialect = sql_dialect
        elif settings_obj:
            self._sql_dialect = getattr(self._settings_obj, "_sql_dialect")
        else:
            self._sql_dialect = None

        self.input_name_as_tree = self.parse_input_name_to_sqlglot_tree()

        # Make sure the identifier (column name) is quoted in all cases
        self.input_name_as_tree.find(exp.Identifier).set(arg="quoted", value=True)

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
            tree = sqlglot.parse_one(self.input_name)
        except ParseError:
            tree = sqlglot.parse_one(f'"{self.input_name}"')

        tree_signature = sqlglot_tree_signature(tree)
        valid_signatures = ["column identifier", "bracket column literal identifier"]

        if tree_signature in valid_signatures:
            return tree
        else:
            # e.g. SUR name parses to 'alias column identifier identifier'
            # but we want "SUR name"
            tree = sqlglot.parse_one(f'"{self.input_name}"')
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

    def add_suffix(self, tree, suffix):
        tree = tree.copy()
        identifier_string = tree.find(exp.Identifier).this
        identifier_string = f"{identifier_string}{suffix}"
        tree.find(exp.Identifier).set(arg="this", value=identifier_string)
        return tree

    def add_prefix(self, tree, prefix):
        tree = tree.copy()
        identifier_string = tree.find(exp.Identifier).this
        identifier_string = f"{prefix}{identifier_string}"
        tree.find(exp.Identifier).set(arg="this", value=identifier_string)
        return tree

    def add_table(self, tree, tablename):
        tree = tree.copy()
        table_identifier = exp.Identifier(this=tablename, quoted=True)
        identifier = tree.find(exp.Column)
        identifier.set(arg="table", value=table_identifier)
        return tree

    @property
    def tf_prefix(self):
        return self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

    def name(self):
        return self.input_name_as_tree.sql()

    def name_l(self):
        return self.add_suffix(self.input_name_as_tree, suffix="_l").sql()

    def name_r(self):
        return self.add_suffix(self.input_name_as_tree, suffix="_l").sql()

    def names_l_r(self):
        return [self.name_l(), self.name_r()]

    def l_name_as_l(self):
        name_with_l_table = self.add_table(self.input_name_as_tree, "l").sql()
        return f"{name_with_l_table} as {self.name_l()}"

    def r_name_as_r(self):
        name_with_r_table = self.add_table(self.input_name_as_tree, "r").sql()
        return f"{name_with_r_table} as {self.name_l()}"

    def l_r_names_as_l_r(self):
        return [self.l_name_as_l(), self.r_name_as_r()]

    def bf_name(self):
        return self.add_prefix(self.input_name_as_tree, suffix=self.bf_prefix).sql()

    @property
    def has_tf_adjustment(self):
        if self._has_tf_adjustments is not None:
            return self._has_tf_adjustments

        if self._settings_obj:
            if self.input_name in self._settings_obj._term_frequency_columns:
                return True
        return False

    def tf_name(self):

        tf_pref = self.tf_prefix
        if self.has_tf_adjustment:
            return add_prefix_or_suffix_to_colname(self.col, prefix=tf_pref)

    def tf_name_l(self):
        if self.has_tf_adjustment:
            return add_prefix_or_suffix_to_colname(self.tf_name(), suffix="_l")

    def tf_name_r(self):
        if self.has_tf_adjustment:
            return add_prefix_or_suffix_to_colname(self.tf_name(), suffix="_r")

    def tf_name_l_r(self):
        if self.has_tf_adjustment:
            return [self.tf_name_l(), self.tf_name_r()]
        else:
            return []

    def l_tf_name_as_l(self):
        if self.has_tf_adjustment:
            return f"l.{self.tf_name()} as {self.tf_name_l()}"

    def r_tf_name_as_r(self):
        if self.has_tf_adjustment:
            return f"r.{self.tf_name()} as {self.tf_name_r()}"

    def l_r_tf_names_as_l_r(self):
        if self.has_tf_adjustment:
            return [self.l_tf_name_as_l(), self.r_tf_name_as_r()]
        else:
            return []

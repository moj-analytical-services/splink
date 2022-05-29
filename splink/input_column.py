import sqlglot
from sqlglot.expressions import Column, Identifier

from .default_from_jsonschema import default_value_from_schema


def _detect_if_name_needs_escaping(name):
    if name in ("group", "index"):
        return True
    tree = sqlglot.parse_one(name)
    if isinstance(tree, Column) and isinstance(tree.this, Identifier):
        return False
    else:
        return True


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

        self._name_needs_escaping = _detect_if_name_needs_escaping(name)

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

    def _escape_if_requested(self, column_name, escape):
        if not escape:
            return column_name
        if self._name_needs_escaping:
            # Create parse tree
            parsed = sqlglot.parse_one("col")

            # Quote it and replace 'col' with true column_name
            parsed.this.args["quoted"] = True
            parsed.this.args["this"] = column_name

            return parsed.sql(dialect=self._sql_dialect)
        else:
            return column_name

    def name(self, escape=True):
        return self._escape_if_requested(self.input_name, escape)

    def name_l(self, escape=True):
        return self._escape_if_requested(f"{self.input_name}_l", escape)

    def name_r(self, escape=True):
        return self._escape_if_requested(f"{self.input_name}_r", escape)

    def names_l_r(self, escape=True):
        return [self.name_l(escape), self.name_r(escape)]

    def l_name_as_l(self, escape=True):
        return f"l.{self.name(escape)} as {self.name_l(escape)}"

    def r_name_as_r(self, escape=True):
        return f"r.{self.name(escape)} as {self.name_r(escape)}"

    def l_r_names_as_l_r(self, escape=True):
        return [self.l_name_as_l(escape), self.r_name_as_r(escape)]

    def bf_name(self, escape=True):
        bf_prefix = self.from_settings_obj_else_default(
            "_bf_prefix", "bayes_factor_column_prefix"
        )
        return self._escape_if_requested(f"{bf_prefix}{self.input_name}", escape)

    @property
    def has_tf_adjustment(self):
        if self._has_tf_adjustments is not None:
            return self._has_tf_adjustments

        if self._settings_obj:
            if self.input_name in self._settings_obj._term_frequency_columns:
                return True
        return False

    def tf_name(self, escape=True):

        tf_prefix = self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

        name = f"{tf_prefix}{self.input_name}"

        if self.has_tf_adjustment:
            return self._escape_if_requested(name, escape)

    def tf_name_l(self, escape=True):
        if self.has_tf_adjustment:
            return self._escape_if_requested(f"{self.tf_name(escape=False)}_l", escape)

    def tf_name_r(self, escape=True):
        if self.has_tf_adjustment:
            return self._escape_if_requested(f"{self.tf_name(escape=False)}_r", escape)

    def tf_name_l_r(self, escape=True):
        if self.has_tf_adjustment:
            return [self.tf_name_l(escape), self.tf_name_r(escape)]
        else:
            return []

    def l_tf_name_as_l(self, escape=True):
        if self.has_tf_adjustment:
            return f"l.{self.tf_name(escape)} as {self.tf_name_l(escape)}"

    def r_tf_name_as_r(self, escape=True):
        if self.has_tf_adjustment:
            return f"r.{self.tf_name(escape)} as {self.tf_name_r(escape)}"

    def l_r_tf_names_as_l_r(self, escape=True):
        if self.has_tf_adjustment:
            return [self.l_tf_name_as_l(escape), self.r_tf_name_as_r(escape)]
        else:
            return []

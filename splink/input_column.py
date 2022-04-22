from .default_from_jsonschema import default_value_from_schema
from .misc import escape_column


def escape_if_requested(col, escape=True):
    if escape:
        return escape_column(col)
    else:
        return col


class InputColumn:
    def __init__(self, name, tf_adjustments=False, settings_obj=None):

        # If settings_obj is None, then default values will be used
        # from the jsonschame
        self.settings_obj = settings_obj
        self.has_tf_adjustments = tf_adjustments

        if name.endswith("_l"):
            self.input_name = name[:-2]
        elif name.endswith("_r"):
            self.input_name = name[:-2]
        else:
            self.input_name = name

    def from_settings_obj_else_default(self, key, schema_key=None):
        # Covers the case where no settings obj is set on the comparison level
        if self.settings_obj:
            return getattr(self.settings_obj, key)
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

    def name(self, escape=True):
        return escape_if_requested(self.input_name, escape)

    def name_l(self, escape=True):
        return escape_if_requested(f"{self.input_name}_l", escape)

    def name_r(self, escape=True):
        return escape_if_requested(f"{self.input_name}_r", escape)

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
        return escape_if_requested(f"{bf_prefix}{self.input_name}", escape)

    @property
    def has_tf_adjustment(self):
        if self.settings_obj:
            if self.input_name in self.settings_obj._term_frequency_columns:
                return True
        return False

    def tf_name(self, escape=True):

        tf_prefix = self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )

        name = f"{tf_prefix}{self.input_name}"

        if self.has_tf_adjustment:
            return escape_if_requested(name, escape)

    def tf_name_l(self, escape=True):
        if self.has_tf_adjustment:
            return escape_if_requested(f"{self.tf_name(escape=False)}_l", escape)

    def tf_name_r(self, escape=True):
        if self.has_tf_adjustment:
            return escape_if_requested(f"{self.tf_name(escape=False)}_r", escape)

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

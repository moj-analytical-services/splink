from .default_from_jsonschema import default_value_from_schema
from .misc import escape_column


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

    @property
    def name(self):
        return escape_column(self.input_name)

    @property
    def name_l(self):
        return escape_column(f"{self.input_name}_l")

    @property
    def name_r(self):
        return escape_column(f"{self.input_name}_r")

    @property
    def names_l_r(self):
        return [self.name_l, self.name_r]

    @property
    def l_name_as_l(self):
        return f"l.{self.name} as {self.name_l}"

    @property
    def r_name_as_r(self):
        return f"r.{self.name} as {self.name_r}"

    @property
    def l_r_names_as_l_r(self):
        return [self.l_name_as_l, self.r_name_as_r]

    @property
    def bf_name(self):
        bf_prefix = self.from_settings_obj_else_default(
            "_bf_prefix", "bayes_factor_column_prefix"
        )
        return escape_column(f"{bf_prefix}{self.input_name}")

    @property
    def has_tf_adjustment(self):
        if self.settings_obj:
            if self.input_name in self.settings_obj._term_frequency_columns:
                return True
        return False

    @property
    def _tf_name_unesc(self):
        tf_prefix = self.from_settings_obj_else_default(
            "_tf_prefix", "term_frequency_adjustment_column_prefix"
        )
        return f"{tf_prefix}{self.input_name}"

    @property
    def tf_name(self):
        if self.has_tf_adjustment:
            return escape_column(self._tf_name_unesc)

    @property
    def tf_name_l(self):
        if self.has_tf_adjustment:
            return escape_column(f"{self._tf_name_unesc}_l")

    @property
    def tf_name_r(self):
        if self.has_tf_adjustment:
            return escape_column(f"{self._tf_name_unesc}_r")

    @property
    def tf_name_l_r(self):
        if self.has_tf_adjustment:
            return [self.tf_name_l, self.tf_name_r]
        else:
            return []

    @property
    def l_tf_name_as_l(self):
        if self.has_tf_adjustment:
            return f"l.{self.tf_name} as {self.tf_name_l}"

    @property
    def r_tf_name_as_r(self):
        if self.has_tf_adjustment:
            return f"r.{self.tf_name} as {self.tf_name_r}"

    @property
    def l_r_tf_names_as_l_r(self):
        if self.has_tf_adjustment:
            return [self.l_tf_name_as_l, self.r_tf_name_as_r]
        else:
            return []

# import sqlglot
# import sqlglot.expressions as exp
# from sqlglot.expressions import Column, Identifier, Bracket

# from .default_from_jsonschema import default_value_from_schema
# from splink.sql_transform import add_prefix_or_suffix_to_colname


# def _detect_if_name_needs_escaping(name):
#     if name in ("group", "index"):
#         return True
#     tree = sqlglot.parse_one(name)
#     if isinstance(tree, Bracket):
#         tree = tree.this

#     if isinstance(tree, Column) and isinstance(tree.this, Identifier):
#         return False
#     else:
#         return True


# class InputColumn:
#     def __init__(self, name, tf_adjustments=False, settings_obj=None, sql_dialect=None):

#         # If settings_obj is None, then default values will be used
#         # from the jsonschama
#         self._settings_obj = settings_obj
#         self._has_tf_adjustments = tf_adjustments

#         self.input_name = name

#         if sql_dialect:
#             self._sql_dialect = sql_dialect
#         elif settings_obj:
#             self._sql_dialect = getattr(self._settings_obj, "_sql_dialect")
#         else:
#             self._sql_dialect = None

#         self._name_needs_escaping = _detect_if_name_needs_escaping(name)

#     def from_settings_obj_else_default(self, key, schema_key=None):
#         # Covers the case where no settings obj is set on the comparison level
#         if self._settings_obj:
#             return getattr(self._settings_obj, key)
#         else:
#             if not schema_key:
#                 schema_key = key
#             return default_value_from_schema(schema_key, "root")

#     @property
#     def col(self):
#         if self._name_needs_escaping:
#             return exp.Column(this=exp.Identifier(this=self.input_name, quoted=False))
#         else:
#             return self.input_name

#     @property
#     def gamma_prefix(self):
#         return self.from_settings_obj_else_default(
#             "_gamma_prefix", "comparison_vector_value_column_prefix"
#         )

#     @property
#     def bf_prefix(self):
#         return self.from_settings_obj_else_default(
#             "_bf_prefix", "bayes_factor_column_prefix"
#         )

#     @property
#     def tf_prefix(self):
#         return self.from_settings_obj_else_default(
#             "_tf_prefix", "term_frequency_adjustment_column_prefix"
#         )

#     def _escape(self, e=True):
#         # Allows for overriding of self._escape_needed.
#         return min(e, self._name_needs_escaping)

#     def name(self, escape=True):
#         return add_prefix_or_suffix_to_colname(self.col, self._escape(escape))

#     def name_l(self, escape=True):
#         return add_prefix_or_suffix_to_colname(
#             self.col, self._escape(escape), suffix="_l"
#         )

#     def name_r(self, escape=True):
#         return add_prefix_or_suffix_to_colname(
#             self.col, self._escape(escape), suffix="_r"
#         )

#     def names_l_r(self, escape=True):
#         e = self._escape(escape)
#         return [self.name_l(e), self.name_r(e)]

#     def l_name_as_l(self, escape=True):
#         e = self._escape(escape)
#         return f"l.{self.name(e)} as {self.name_l(e)}"

#     def r_name_as_r(self, escape=True):
#         e = self._escape(escape)
#         return f"r.{self.name(e)} as {self.name_r(e)}"

#     def l_r_names_as_l_r(self, escape=True):
#         e = self._escape(escape)
#         return [self.l_name_as_l(e), self.r_name_as_r(e)]

#     def bf_name(self, escape=True):
#         bf_pref = self.bf_prefix
#         return add_prefix_or_suffix_to_colname(
#             self.col, self._escape(escape), prefix=bf_pref
#         )

#     @property
#     def has_tf_adjustment(self):
#         if self._has_tf_adjustments is not None:
#             return self._has_tf_adjustments

#         if self._settings_obj:
#             if self.input_name in self._settings_obj._term_frequency_columns:
#                 return True
#         return False

#     def tf_name(self, escape=True):

#         tf_pref = self.tf_prefix
#         if self.has_tf_adjustment:
#             return add_prefix_or_suffix_to_colname(
#                 self.col, self._escape(escape), prefix=tf_pref
#             )

#     def tf_name_l(self, escape=True):
#         if self.has_tf_adjustment:
#             return add_prefix_or_suffix_to_colname(
#                 self.tf_name(escape=False), self._escape(escape), suffix="_l"
#             )

#     def tf_name_r(self, escape=True):
#         if self.has_tf_adjustment:
#             return add_prefix_or_suffix_to_colname(
#                 self.tf_name(escape=False), self._escape(escape), suffix="_r"
#             )

#     def tf_name_l_r(self, escape=True):
#         if self.has_tf_adjustment:
#             return [self.tf_name_l(escape), self.tf_name_r(escape)]
#         else:
#             return []

#     def l_tf_name_as_l(self, escape=True):
#         if self.has_tf_adjustment:
#             return f"l.{self.tf_name(escape)} as {self.tf_name_l(escape)}"

#     def r_tf_name_as_r(self, escape=True):
#         if self.has_tf_adjustment:
#             return f"r.{self.tf_name(escape)} as {self.tf_name_r(escape)}"

#     def l_r_tf_names_as_l_r(self, escape=True):
#         if self.has_tf_adjustment:
#             return [self.l_tf_name_as_l(escape), self.r_tf_name_as_r(escape)]
#         else:
#             return []


class InputColumn:
    def __init__(self, name, tf_adjustments=False, settings_obj=None, sql_dialect=None):
        self.parsed_col = sqlglot.parse_one(name)

        # Check whether escape is needed by attempting to parse and asking whether the
        # tree starts with a bracket or column

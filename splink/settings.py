from copy import deepcopy
from math import log2
import re

from .validate import get_default_value_from_schema

from .charts import load_chart_definition, altair_if_installed_else_json


class ComparisonLevel:
    def __init__(self, level_dict, comparison_column_obj=None, settings_obj=None):
        self.level_dict = level_dict
        self.comparison_column = comparison_column_obj
        self.settings = settings_obj

    def __getitem__(self, i):
        return self.level_dict[i]

    @property
    def is_null(self):
        vector_value = self.comparison_vector_value
        if vector_value:
            if str(vector_value) == "-1":
                return True
            else:
                return False
        else:
            return None

    @property
    def comparison_vector_value(self):
        sql_expr = self.level_dict.get("sql_expr")

        if sql_expr:
            # sql_expr is either in the form 'when ... then x' or 'else y'
            # we want to extract the value of x or y
            # sqlglot needs a full sql case expression, it can't parse e.g. 'else 0'

            # capture value after else using regex
            sql_expr = sql_expr.strip()
            if sql_expr.lower().startswith("else"):
                return sql_expr[4:].strip()

            # https://stackoverflow.com/a/33233868/1779128
            case_value = re.search(r"(?s:.*)then (.*)$", sql_expr, re.IGNORECASE).group(
                1
            )
            if case_value:
                return case_value.strip()
        else:
            return None

    @property
    def not_null(self):
        if self.is_null is None:
            return None
        return not self.is_null

    @property
    def has_m_u(self):

        if self.is_null:
            return True

        m = self.level_dict.get("m_probability")
        u = self.level_dict.get("u_probability")

        if m and u:
            return True

        return False

    @property
    def m(self):
        if self.is_null:
            return 1

        if "m_probability" in self.level_dict:
            return self.level_dict["m_probability"]
        return None

    @property
    def u(self):
        if self.is_null:
            return 1

        if "u_probability" in self.level_dict:
            return self.level_dict["u_probability"]
        return None

    @property
    def bayes_factor(self):
        if self.has_m_u and self.u != 0:
            return self.m / self.u
        else:
            return None

    @property
    def log2_bayes_factor(self):
        bf = self.bayes_factor
        if bf and self.m != 0:
            return log2(bf)

    @property
    def gamma_index(self):
        if "gamma_index" in self.level_dict:
            return self.level_dict["gamma_index"]
        else:
            return None

    @property
    def proportion_of_nonnull_records_in_level(self):

        if self.settings:
            sd = self.settings.settings_dict
            lam = sd.get("proportion_of_matches")

        if lam and self.has_m_u:
            return self.m * lam + self.u * (1 - lam)
        else:
            return None

    def as_dict(self):

        d = {}
        d["label"] = self["label"]

        d["sql_expr"] = self["sql_expr"]

        d["gamma_column_name"] = f"gamma_{self.comparison_column.name}"
        d["level_name"] = self["label"]

        d["gamma_index"] = self.gamma_index
        d["comparison_vector_value"] = self.comparison_vector_value
        d["column_name"] = self.comparison_column.name
        d["num_levels"] = self.comparison_column.num_levels

        d["m_probability"] = self.m
        d["u_probability"] = self.u
        d["bayes_factor"] = self.bayes_factor
        d["log2_bayes_factor"] = self.log2_bayes_factor

        d["level_proportion"] = self.proportion_of_nonnull_records_in_level
        d["is_null"] = self.is_null

        return d


class ComparisonColumn:
    def __init__(self, column_dict, settings_obj=None):
        self.column_dict = column_dict
        self.settings_obj = settings_obj

    def __getitem__(self, i):
        return self.column_dict[i]

    def _dict_key_else_default_value(self, key):
        cd = self.column_dict

        if key in cd:
            return cd[key]
        else:
            return get_default_value_from_schema(key, True)

    @property
    def custom_comparison(self):
        cd = self.column_dict
        if "custom_name" in cd:
            return True
        elif "col_name" in cd:
            return False

    @property
    def columns_used(self):
        cd = self.column_dict
        if "col_name" in cd:
            return [cd["col_name"]]
        elif "custom_name" in cd:
            return cd["custom_columns_used"]

    @property
    def name(self):
        cd = self.column_dict
        if "custom_name" in cd:
            return cd["custom_name"]
        elif "col_name" in cd:
            return cd["col_name"]

    @property
    def has_case_expression_or_comparison_levels(self):
        cd = self.column_dict
        if "case_expression" in cd:
            return True
        elif "comparison_levels" in cd:
            return True
        else:
            return False

    @property
    def gamma_name(self):
        return f"gamma_{self.name}"

    @property
    def num_levels(self):
        cd = self.column_dict

        cld = self.comparison_levels_dict
        if cld:
            comparison_levels_keys = cld.keys()

            comparison_levels_keys = [
                k for k in comparison_levels_keys if str(k) != "-1"
            ]
            return len(comparison_levels_keys)
        if "num_levels" in cd:
            return cd["num_levels"]

    @property
    def input_cols_used(self):
        cd = self.column_dict
        if "custom_name" in cd:
            return cd["custom_columns_used"]
        elif "col_name" in cd:
            return [cd["col_name"]]

    def _attach_m_u_to_comparison_levels(self, comparison_levels):

        m_probabilities = self.column_dict.get("m_probabilities")
        if m_probabilities:

            for level in comparison_levels:
                cl = ComparisonLevel(level)
                m_index = int(cl.comparison_vector_value)
                if cl.not_null:
                    level["m_probability"] = m_probabilities[m_index]

        u_probabilities = self.column_dict.get("u_probabilities")
        if u_probabilities:
            counter = 0
            for level in comparison_levels:
                cl = ComparisonLevel(level)
                u_index = int(cl.comparison_vector_value)
                if cl.not_null:
                    level["u_probability"] = u_probabilities[u_index]

        return comparison_levels

    @property
    def comparison_levels_dict(self):
        cd = self.column_dict

        if "comparison_levels" not in cd:
            return None

        comparison_levels = deepcopy(cd["comparison_levels"])

        results = {}
        for level in comparison_levels:
            cl = ComparisonLevel(level, self, self.settings_obj)
            key = cl.comparison_vector_value
            results[key] = cl
            cl.level_dict["gamma_index"] = int(key)

        comparison_levels = self._attach_m_u_to_comparison_levels(comparison_levels)
        return results

    @property
    def comparison_levels_list(self):
        return list(self.comparison_levels_dict.values())

    @property
    def term_frequency_adjustments(self):
        cd = self.column_dict
        return cd["term_frequency_adjustments"]

    def df_e_row_intuition_dict(self, row_dict):

        gamma_value = str(int(row_dict[self.gamma_name]))
        cl = self.comparison_levels_dict[gamma_value]
        row_desc = cl.as_dict()

        row_desc["value_l"] = ", ".join(
            [str(row_dict[c + "_l"]) for c in self.columns_used]
        )
        row_desc["value_r"] = ", ".join(
            [str(row_dict[c + "_r"]) for c in self.columns_used]
        )

        return row_desc

    def set_m_probability(self, level: int, prob: float, force: bool = False):
        cd = self.column_dict
        fixed = self._dict_key_else_default_value("fix_m_probabilities")
        if not fixed or force:
            cd["m_probabilities"][level] = prob

    def set_u_probability(self, level: int, prob: float, force: bool = False):
        cd = self.column_dict
        fixed = self._dict_key_else_default_value("fix_u_probabilities")
        if not fixed or force:
            cd["u_probabilities"][level] = prob

    def reset_probabilities(self, force: bool = False):
        cd = self.column_dict
        fixed_m = self._dict_key_else_default_value("fix_m_probabilities")
        fixed_u = self._dict_key_else_default_value("fix_u_probabilities")
        if not fixed_m or force:
            if "m_probabilities" in cd:
                cd["m_probabilities"] = [None for c in cd["m_probabilities"]]

        if not fixed_u or force:
            if "u_probabilities" in cd:
                cd["u_probabilities"] = [None for c in cd["u_probabilities"]]

    def as_rows(self, proportion_of_matches=None):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        # This should iterate over the `comparison_levels` dict
        for comparison_level in self.comparison_levels_list:
            r = comparison_level.as_dict()
            rows.append(r)
        return rows

    def _repr_pretty_(self, p, cycle):

        p.text("------------------------------------")
        p.break_()
        p.text(f"Comparison of {self.name}")
        p.break_()
        for row in self.as_rows():
            p.text(f"{row['level_name']}")
            p.break_()
            value_m = (
                f"{row['m_probability']*100:.3g}%" if row["m_probability"] else "None"
            )
            p.text(f"   Proportion in level amongst matches:       {value_m}")
            p.break_()
            value_u = (
                f"{row['u_probability']*100:.3g}%" if row["u_probability"] else "None"
            )
            p.text(f"   Proportion in level amongst non-matches:   {value_u}%")
            p.break_()
            value_b = (
                f"{row['bayes_factor']*100:.3g}%" if row["bayes_factor"] else "None"
            )
            p.text(f"   Bayes factor:                              {value_b}")
            p.break_()


class Settings:
    def __init__(self, settings_dict):
        self.settings_dict = deepcopy(settings_dict)

    def __getitem__(self, i):
        return self.settings_dict[i]

    def __setitem__(self, key, value):
        self.settings_dict[key] = value

    def complete_settings_dict(self, spark):
        """Complete all fields in the setting dictionary
        taking values from defaults"""
        from .default_settings import complete_settings_dict

        self.settings_dict = complete_settings_dict(self.settings_dict, spark)

    @property
    def comparison_column_dict(self):
        sd = self.settings_dict
        lookup = {}
        for c in sd["comparison_columns"]:

            cc = ComparisonColumn(c, self)
            name = cc.name
            lookup[name] = cc
        return lookup

    @property
    def comparison_columns_list(self):
        return list(self.comparison_column_dict.values())

    @property
    def any_cols_have_tf_adjustments(self):
        return any([c.term_frequency_adjustments for c in self.comparison_columns_list])

    def get_comparison_column(self, col_name_or_custom_name):
        if col_name_or_custom_name in self.comparison_column_dict:
            return self.comparison_column_dict[col_name_or_custom_name]
        else:
            raise KeyError(
                f"You requested comparison column {col_name_or_custom_name}"
                " but it does not exist in the settings object"
            )

    def reset_all_probabilities(self, force: bool = False):
        sd = self.settings_dict
        sd["proportion_of_matches"] = None
        for c in self.comparison_columns_list:
            c.reset_probabilities(force=force)

    def m_u_as_rows(self, drop_null=True):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for cc in self.comparison_columns_list:
            rows.extend(cc.as_rows())
        if drop_null:
            rows = [r for r in rows if not r["is_null"]]
        return rows

    def set_m_probability(
        self, name: str, level: int, prob: float, force: bool = False
    ):
        cc = self.get_comparison_column(name)
        cc.set_m_probability(level, prob, force)

    def set_u_probability(
        self, name: str, level: int, prob: float, force: bool = False
    ):
        cc = self.get_comparison_column(name)
        cc.set_u_probability(level, prob, force)

    def overwrite_m_u_probs_from_other_settings_dict(
        self, incoming_settings_dict, overwrite_m=True, overwrite_u=True
    ):
        """Overwrite the m and u probabilities with
        the values from another settings dict where comparison column exists
        in this settings object
        """
        incoming_settings_obj = Settings(incoming_settings_dict)

        for cc_incoming in incoming_settings_obj.comparison_columns_list:
            if cc_incoming.name in self.comparison_column_dict:
                cc_existing = self.get_comparison_column(cc_incoming.name)

                if overwrite_m:
                    if "m_probabilities" in cc_incoming.column_dict:
                        cc_existing.column_dict["m_probabilities"] = cc_incoming[
                            "m_probabilities"
                        ]

                if overwrite_u:
                    if "u_probabilities" in cc_incoming.column_dict:
                        cc_existing.column_dict["u_probabilities"] = cc_incoming[
                            "u_probabilities"
                        ]

    def remove_comparison_column(self, name):
        removed = False
        new_ccs = []
        for cc in self.comparison_columns_list:
            if cc.name != name:
                new_ccs.append(cc.column_dict)
            else:
                removed = True
        self.settings_dict["comparison_columns"] = new_ccs
        if not removed:
            raise ValueError(f"Could not find a column named {name}")

    def probability_distribution_chart(self):  # pragma: no cover
        chart_path = "probability_distribution_chart.json"
        chart = load_chart_definition(chart_path)
        chart["data"]["values"] = self.m_u_as_rows()
        chart["title"]["subtitle"] += f" {self['proportion_of_matches']:.3g}"

        return altair_if_installed_else_json(chart)

    def bayes_factor_chart(self):  # pragma: no cover
        chart_path = "bayes_factor_chart_def.json"
        chart = load_chart_definition(chart_path)
        rows = self.m_u_as_rows()

        chart["data"]["values"] = rows
        return altair_if_installed_else_json(chart)

    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        if cycle:
            p.text("ComparisonColumn()")
        else:
            value = (
                f"{self['proportion_of_matches']:.4g}"
                if self["proportion_of_matches"]
                else "None"
            )
            p.text(f"λ (proportion of matches) = {value}")
            for c in self.comparison_columns_list:
                p.break_()
                p.pretty(c)

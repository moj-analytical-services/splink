from .default_settings import complete_settings_dict
from .validate import _get_default_value
from copy import deepcopy
from math import log2
from .charts import load_chart_definition, altair_if_installed_else_json


class ComparisonColumn:
    def __init__(self, column_dict):
        self.column_dict = column_dict

    def __getitem__(self, i):
        return self.column_dict[i]

    def _dict_key_else_default_value(self, key):
        cd = self.column_dict

        if key in cd:
            return cd[key]
        else:
            return _get_default_value(key, True)

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
    def gamma_name(self):
        return f"gamma_{self.name}"

    @property
    def num_levels(self):
        cd = self.column_dict
        m_probs = cd["m_probabilities"]
        u_probs = cd["u_probabilities"]
        if len(m_probs) == len(u_probs):
            return len(m_probs)
        else:
            raise ValueError("Length of m and u probs unequal")

    @property
    def max_gamma_index(self):
        return self.num_levels - 1

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
            if "m_probablities" in cd:
                cd["m_probabilities"] = [None for c in cd["m_probabilities"]]
        if not fixed_u or force:
            if "u_probabilities" in cd:
                cd["u_probabilities"] = [None for c in cd["u_probabilities"]]

    def _level_as_dict(self, gamma_index, proportion_of_matches):

        d = {}
        m_prob = self["m_probabilities"][gamma_index]
        u_prob = self["u_probabilities"][gamma_index]

        d["gamma"] = f"gamma_{self.name}"
        d["value_of_gamma"] = f"level_{gamma_index}"
        d["m_probability"] = m_prob
        d["u_probability"] = u_prob
        d["gamma_index"] = gamma_index
        d["column"] = self.name
        d["max_gamma_index"] = self.num_levels - 1

        if proportion_of_matches:
            lam = proportion_of_matches
            d["level_proportion"] = m_prob * lam + u_prob * (1 - lam)
        else:
            d["level_proportion"] = None

        try:
            d["bayes_factor"] = m_prob / u_prob
            d["log2_bayes_factor"] = log2(d["bayes_factor"])
        except ZeroDivisionError:
            d["bayes_factor"] = None
            d["log2_bayes_factor"] = None
        return d

    def as_rows(self, proportion_of_matches=None):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for gamma_index in range(self.num_levels):
            r = self._level_as_dict(gamma_index, proportion_of_matches)
            rows.append(r)
        return rows


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
        self.settings_dict = complete_settings_dict(self.settings_dict, spark)

    @property
    def _comparison_column_lookup(self):
        sd = self.settings_dict
        lookup = {}
        for i, c in enumerate(sd["comparison_columns"]):
            c["gamma_index"] = i
            cc = ComparisonColumn(c)
            name = cc.name
            lookup[name] = cc
        return lookup

    @property
    def comparison_columns(self):
        return self._comparison_column_lookup.values()

    def get_comparison_column(self, col_name_or_custom_name):
        return self._comparison_column_lookup[col_name_or_custom_name]

    def reset_all_probabilities(self, force: bool = False):
        sd = self.settings_dict
        sd["proportion_of_matches"] = None
        for c in self.comparison_columns:
            c.reset_probabilities(force=force)

    def m_u_as_rows(self):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for c in self.comparison_columns:
            rows.extend(c.as_rows(self["proportion_of_matches"]))
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

    def probability_distribution_chart(self):  # pragma: no cover
        chart_path = "probability_distribution_chart.json"
        chart = load_chart_definition(chart_path)
        chart["data"]["values"] = self.m_u_as_rows()
        return altair_if_installed_else_json(chart)

    def bayes_factor_chart(self):  # pragma: no cover
        chart_path = "bayes_factor_chart_def.json"
        chart = load_chart_definition(chart_path)
        chart["data"]["values"] = self.m_u_as_rows()
        return altair_if_installed_else_json(chart)

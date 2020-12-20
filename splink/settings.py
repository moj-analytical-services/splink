import sys

sys.path.insert(0, "/Users/robinlinacre/Documents/data_linking/splink")
from splink.default_settings import complete_settings_dict
from splink.validate import _get_default_value
from copy import deepcopy


def _normalise_prob_list(prob_array: list):
    sum_list = sum(prob_array)
    return [i / sum_list for i in prob_array]


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
            return [cd["custom_columns_used"]]

    @property
    def u_probabilities_normalised(self):
        cd = self.column_dict
        return _normalise_prob_list(cd["u_probabilities"])

    @property
    def name(self):
        cd = self.column_dict
        if "custom_name" in cd:
            return cd["custom_name"]
        elif "col_name" in cd:
            return cd["col_name"]

    @property
    def gamma_column_name(self):
        return f"gamma_{self.name}"

    @property
    def m_probabilities_normalised(self):
        cd = self.column_dict
        return _normalise_prob_list(cd["m_probabilities"])

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
            del cd["m_probabilities"]
        if not fixed_u or force:
            del cd["u_probabilities"]

    def _get_row_absolute(self, prob, gamma_index, match):
        return {
            "gamma": f"gamma_{self.name}",
            "match": match,
            "value_of_gamma": f"level_{gamma_index}",
            "probability": prob,
            "gamma_index": gamma_index,
            "column": self.name,
        }

    def _get_row_comparative(self, prob_m, prob_u, gamma_index, proportion_of_matches):
        lam = proportion_of_matches

        row = {
            "gamma": f"gamma_{self.name}",
            "value_of_gamma": f"level_{gamma_index}",
            "gamma_index": gamma_index,
            "column": self.name,
        }

        row["level_proportion"] = prob_m * lam + prob_u * (1 - lam)
        try:
            row["bayes_factor"] = prob_m / prob_u
        except ZeroDivisionError:
            row["bayes_factor"] = None
        return row

    def as_rows_absolute(self):
        """Convert to rows e.g. to use to plot
        in a chart"""

        rows = []
        cd = self.column_dict
        zipped = zip(cd["m_probabilities"], cd["u_probabilities"])

        for gamma_index, (prob_m, prob_u) in enumerate(zipped):
            r = self._get_row_absolute(prob_m, gamma_index, 1)
            rows.append(r)
            r = self._get_row_absolute(prob_u, gamma_index, 0)
            rows.append(r)
        return rows

    def as_rows_comparative(self, proportion_of_matches):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        cd = self.column_dict
        zipped = zip(cd["m_probabilities"], cd["u_probabilities"])
        for gamma_index, (prob_m, prob_u) in enumerate(zipped):
            r = self._get_row_comparative(
                prob_m, prob_u, gamma_index, proportion_of_matches
            )
            rows.append(r)
        return rows


class Settings:
    def __init__(self, settings_dict):
        self.settings_dict = deepcopy(settings_dict)

    def __getitem__(self, i):
        return self.settings_dict[i]

    def complete_settings_dict(self, spark):
        """Complete all fields in the setting dictionary
        taking values from defaults"""
        self.settings_dict = complete_settings_dict(self.settings_dict, spark)

    @property
    def _comparison_col_dict_lookup(self):
        sd = self.settings_dict
        lookup = {}
        for i, c in enumerate(sd["comparison_columns"]):
            name = c.get("col_name", c.get("custom_name"))
            c["gamma_index"] = i
            lookup[name] = c
        return lookup

    @property
    def comparison_columns(self):
        sd = self.settings_dict
        return [ComparisonColumn(c) for c in sd["comparison_columns"]]

    def get_comparison_column(self, col_name_or_custom_name):
        col_dict = self._comparison_col_dict_lookup["col_name_or_custom_name"]
        return ComparisonColumn(col_dict)

    @property
    def as_dict(self):
        return self.settings_dict

    def reset_all_probabilities(self, force: bool = False):
        sd = self.settings_dict
        sd["proportion_of_matches"] = None
        for c in self.comparison_columns:
            c.reset_probabilities(force=force)

    def as_rows_absolute(self):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for c in self.comparison_columns:
            rows.extend(c.as_rows_absolute())
        return rows

    def as_rows_comparative(self):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for c in self.comparison_columns:
            rows.extend(c.as_rows_comparative(self["proportion_of_matches"]))
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

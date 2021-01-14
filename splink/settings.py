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

    @property
    def input_cols_used(self):
        cd = self.column_dict
        if "custom_name" in cd:
            return cd["custom_columns_used"]
        elif "col_name" in cd:
            return [cd["col_name"]]

    def get_m_u_bayes_at_gamma_index(self, gamma_index):

        # if -1 this indicates a null field
        if gamma_index == -1:
            m = 1
            u = 1
        else:
            m = self["m_probabilities"][gamma_index]
            u = self["u_probabilities"][gamma_index]
        if u != 0 and m is not None and u is not None:
            bayes = m / u
            if m != 0:
                log_2_bayes = log2(bayes)
            else:
                log_2_bayes = None
        else:
            bayes = None
            log_2_bayes = None
        return {
            "m_probability": m,
            "u_probability": u,
            "bayes_factor": bayes,
            "log2_bayes_factor": log_2_bayes,
        }

    def describe_row_dict(self, row_dict, proportion_of_matches=None):

        gamma_index = int(row_dict[self.gamma_name])
        row_desc = self.level_as_dict(gamma_index, proportion_of_matches)

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
                cd["m_probabilities"] = [0 for c in cd["m_probabilities"]]

        if not fixed_u or force:
            if "u_probabilities" in cd:
                cd["u_probabilities"] = [0 for c in cd["u_probabilities"]]

    def level_as_dict(self, gamma_index, proportion_of_matches=None):

        d = self.get_m_u_bayes_at_gamma_index(gamma_index)

        d["gamma_column_name"] = f"gamma_{self.name}"
        d["level_name"] = f"level_{gamma_index}"

        d["gamma_index"] = gamma_index
        d["column_name"] = self.name
        d["max_gamma_index"] = self.max_gamma_index
        d["num_levels"] = self.num_levels

        d["level_proportion"] = None
        if proportion_of_matches:
            lam = proportion_of_matches
            m = d["m_probability"]
            u = d["u_probability"]
            d["level_proportion"] = m * lam + u * (1 - lam)

        return d

    def as_rows(self, proportion_of_matches=None):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for gamma_index in range(self.num_levels):
            r = self.level_as_dict(gamma_index, proportion_of_matches)
            rows.append(r)
        return rows

    def __repr__(self):
        lines = []
        lines.append("------------------------------------")
        lines.append(f"Comparison of {self.name}")
        lines.append("")
        for row in self.as_rows():
            lines.append(f"{row['level_name']}")
            lines.append(
                f"   Proportion in level amongst matches:       {row['m_probability']*100:.3g}%"
            )
            lines.append(
                f"   Proportion in level amongst non-matches:   {row['u_probability']*100:.3g}%"
            )
            lines.append(
                f"   Bayes factor:                              {row['bayes_factor']:,.3g}"
            )
        return "\n".join(lines)


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
    def comparison_column_dict(self):
        sd = self.settings_dict
        lookup = {}
        for i, c in enumerate(sd["comparison_columns"]):
            c["gamma_index"] = i
            cc = ComparisonColumn(c)
            name = cc.name
            lookup[name] = cc
        return lookup

    @property
    def comparison_columns_list(self):
        return list(self.comparison_column_dict.values())

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

    def m_u_as_rows(self):
        """Convert to rows e.g. to use to plot
        in a chart"""
        rows = []
        for c in self.comparison_columns_list:
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
        chart["data"]["values"] = self.m_u_as_rows()
        return altair_if_installed_else_json(chart)

    def __repr__(self):  # pragma: no cover

        lines = []
        lines.append(f"λ (proportion of matches) = {self['proportion_of_matches']:.4g}")

        for c in self.comparison_columns_list:
            lines.append(c.__repr__())

        return "\n".join(lines)

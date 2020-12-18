from splink.validate import validate_settings
from copy import deepcopy


def _normalise_prob_list(prob_array: list):
    sum_list = sum(prob_array)
    return [i / sum_list for i in prob_array]


class ComparisonColumn:
    def __init__(self, column_dict):
        self.column_dict = column_dict

    def __getitem__(self, i):
        return self.column_dict[i]

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
    def bayes_factors(self):
        pass

    @property
    def u_probabilities_normalised(self):
        cd = self.column_dict
        return _normalise_prob_list(cd["u_probabilities"])

    @property
    def m_probabilities_normalised(self):
        cd = self.column_dict
        return _normalise_prob_list(cd["m_probabilities"])

    def set_m_probability(self, level: int, prob: float):
        cd = self.column_dict
        cd["m_probabilities"][level] = prob

    def set_u_probability(self, level: int, prob: float):
        cd = self.column_dict
        cd["u_probabilities"][level] = prob

    def reset_probabilities(self):
        cd = self.column_dict
        cd["m_probabilities"] = [None for i in cd["m_probabilities"]]
        cd["u_probabilities"] = [None for i in cd["u_probabilities"]]


class Settings:
    def __init__(self, settings_dict):
        self.settings_dict = deepcopy(settings_dict)

    def __getitem__(self, i):
        return self.settings_dict[i]

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

    def reset_all_probabilities(self):
        sd = self.settings_dict
        sd["proportion_of_matches"] = None
        for c in self.comparison_columns:
            c.reset_probabilities()


settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
        "l.dob = r.dob",
    ],
    "comparison_columns": [
        {"col_name": "first_name", "num_levels": 3, "term_frequency_adjustments": True},
        {"col_name": "surname", "num_levels": 3, "term_frequency_adjustments": True},
        {"col_name": "dob"},
        {"col_name": "city"},
        {"col_name": "email"},
    ],
    "additional_columns_to_retain": ["group"],
    "em_convergence": 0.01,
    "proportion_of_matches": 0.2,
}

s = Settings(settings)

s.params_dict

{
    "λ": 0.3559739887714386,
    "π": {
        "gamma_first_name": {
            "gamma_index": 0,
            "desc": "Comparison of first_name",
            "column_name": "first_name",
            "custom_comparison": False,
            "num_levels": 3,
            "prob_dist_match": {
                "level_0": {"value": 0, "probability": 0.2684518098831177},
                "level_1": {"value": 1, "probability": 0.07068818062543869},
                "level_2": {"value": 2, "probability": 0.660860002040863},
            },
            "prob_dist_non_match": {
                "level_0": {"value": 0, "probability": 0.5639071464538574},
                "level_1": {"value": 1, "probability": 0.006509868428111076},
                "level_2": {"value": 2, "probability": 0.42958298325538635},
            },
            "fix_match_probs": False,
            "fix_non_match_probs": False,
        },
        "gamma_surname": {
            "gamma_index": 1,
            "desc": "Comparison of surname",
            "column_name": "surname",
            "custom_comparison": False,
            "num_levels": 3,
            "prob_dist_match": {
                "level_0": {"value": 0, "probability": 0.2625163495540619},
                "level_1": {"value": 1, "probability": 0.058174461126327515},
                "level_2": {"value": 2, "probability": 0.6793091893196106},
            },
            "prob_dist_non_match": {
                "level_0": {"value": 0, "probability": 0.3866064250469208},
                "level_1": {"value": 1, "probability": 0.004652306903153658},
                "level_2": {"value": 2, "probability": 0.608741283416748},
            },
            "fix_match_probs": False,
            "fix_non_match_probs": False,
        },
        "gamma_dob": {
            "gamma_index": 2,
            "desc": "Comparison of dob",
            "column_name": "dob",
            "custom_comparison": False,
            "num_levels": 2,
            "prob_dist_match": {
                "level_0": {"value": 0, "probability": 0.16022531688213348},
                "level_1": {"value": 1, "probability": 0.8397746682167053},
            },
            "prob_dist_non_match": {
                "level_0": {"value": 0, "probability": 0.890222430229187},
                "level_1": {"value": 1, "probability": 0.1097775399684906},
            },
            "fix_match_probs": False,
            "fix_non_match_probs": False,
        },
        "gamma_city": {
            "gamma_index": 3,
            "desc": "Comparison of city",
            "column_name": "city",
            "custom_comparison": False,
            "num_levels": 2,
            "prob_dist_match": {
                "level_0": {"value": 0, "probability": 0.16518081724643707},
                "level_1": {"value": 1, "probability": 0.8348191976547241},
            },
            "prob_dist_non_match": {
                "level_0": {"value": 0, "probability": 0.8401907086372375},
                "level_1": {"value": 1, "probability": 0.15980930626392365},
            },
            "fix_match_probs": False,
            "fix_non_match_probs": False,
        },
        "gamma_email": {
            "gamma_index": 4,
            "desc": "Comparison of email",
            "column_name": "email",
            "custom_comparison": False,
            "num_levels": 2,
            "prob_dist_match": {
                "level_0": {"value": 0, "probability": 0.10278414934873581},
                "level_1": {"value": 1, "probability": 0.8972158432006836},
            },
            "prob_dist_non_match": {
                "level_0": {"value": 0, "probability": 0.9378442168235779},
                "level_1": {"value": 1, "probability": 0.06215580180287361},
            },
            "fix_match_probs": False,
            "fix_non_match_probs": False,
        },
    },
}

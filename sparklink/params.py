import copy
import json
from pprint import pprint
from .gammas import complete_settings_dict
from .chart_definitions import (
    lambda_iteration_chart_def,
    pi_iteration_chart_def,
    probability_distribution_chart,
    ll_iteration_chart_def,
    multi_chart_template,
)
import random

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


class Params:

    """
    Stores both current model parameters (in self.params)
    and values of params for all previous iterations
    of the model (in self.param_history)
    """

    def __init__(self, gamma_settings, starting_lambda=0.2):
        self.params = {"λ": starting_lambda, "π": {}}

        self.param_history = []

        self.iteration = 1

        self.gamma_settings = complete_settings_dict(gamma_settings)

        self.log_likelihood_exists = False

        self.real_params = None
        self.prob_m_2_levels = [1, 9]
        self.prob_nm_2_levels = [9, 1]
        self.prob_m_3_levels = [1, 2, 7]
        self.prob_nm_3_levels = [7, 2, 1]

        self.generate_param_dict()

    @property
    def gamma_cols(self):
        return self.params["π"].keys()

    def describe_gammas(self):
        return {k: i["desc"] for k, i in self.params["π"].items()}

    def generate_param_dict(self):
        """

        """

        for col_name, col_dict in self.gamma_settings.items():
            i = col_dict["gamma_index"]

            self.params["π"][f"gamma_{i}"] = {}
            self.params["π"][f"gamma_{i}"]["desc"] = f"Comparison of {col_name}"
            self.params["π"][f"gamma_{i}"]["column_name"] = f"{col_name}"

            num_levels = col_dict["levels"]

            prob_dist_match = {}
            prob_dist_non_match = {}

            if num_levels == 2:
                probs_m = self.prob_m_2_levels
                probs_nm = self.prob_nm_2_levels

            elif num_levels == 3:
                probs_m = self.prob_m_3_levels
                probs_nm = self.prob_nm_3_levels
            else:
                probs_m = [random.uniform(0, 1) for r in range(num_levels)]
                probs_nm = [random.uniform(0, 1) for r in range(num_levels)]

            s = sum(probs_m)
            probs_m = [p / s for p in probs_m]

            s = sum(probs_nm)
            probs_nm = [p / s for p in probs_nm]

            for level_num in range(num_levels):
                prob_dist_match[f"level_{level_num}"] = {
                    "value": level_num,
                    "probability": probs_m[level_num],
                }
                prob_dist_non_match[f"level_{level_num}"] = {
                    "value": level_num,
                    "probability": probs_nm[level_num],
                }

            self.params["π"][f"gamma_{i}"]["prob_dist_match"] = prob_dist_match
            self.params["π"][f"gamma_{i}"]["prob_dist_non_match"] = prob_dist_non_match

    def set_pi_value(self, gamma_str, level_int, match_str, prob_float):
        """
        gamma_str e.g. gamma_0
        level_int e.g. 1
        match_str e.g. match or non_match
        prob_float e.g. 0.5

        """
        this_g = self.params["π"][gamma_str]
        this_g[f"prob_dist_{match_str}"][f"level_{level_int}"][
            "probability"
        ] = prob_float

    @staticmethod
    def convert_params_dict_to_data(params, iteration_num=None):
        """
        Convert the params dict into a dataframe

        If iteration_num is specified, this will be turned into a column in the dataframe
        """

        data = []
        for gamma_str, gamma_dict in params["π"].items():

            for level_str, level_dict in gamma_dict["prob_dist_match"].items():
                this_row = {}
                if not iteration_num is None:
                    this_row["iteration"] = iteration_num
                this_row["gamma"] = gamma_str
                this_row["match"] = 1
                this_row["value_of_gamma"] = level_str
                this_row["probability"] = level_dict["probability"]
                this_row["value"] = level_dict["value"]
                this_row["column"] = gamma_dict["column_name"]
                data.append(this_row)

            for level_str, level_dict in gamma_dict["prob_dist_non_match"].items():
                this_row = {}
                if not iteration_num is None:
                    this_row["iteration"] = iteration_num
                this_row["gamma"] = gamma_str
                this_row["match"] = 0
                this_row["value_of_gamma"] = level_str
                this_row["probability"] = level_dict["probability"]
                this_row["value"] = level_dict["value"]
                this_row["column"] = gamma_dict["column_name"]
                data.append(this_row)
        return data

    def iteration_history_df_gammas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.extend(self.convert_params_dict_to_data(param_value, it_num))
        data.extend(self.convert_params_dict_to_data(self.params, it_num + 1))
        return data

    def iteration_history_df_lambdas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append({"λ": param_value["λ"], "iteration": it_num})
        data.append({"λ": self.params["λ"], "iteration": it_num + 1})
        return data

    def iteration_history_df_log_likelihood(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append(
                {"log_likelihood": param_value["log_likelihood"], "iteration": it_num}
            )
        data.append(
            {"log_likelihood": self.params["log_likelihood"], "iteration": it_num + 1}
        )
        return data

    def reset_param_values_to_none(self):
        """
        Reset λ and all probability values to None to ensure we
        don't accidentally re-use old values
        """
        self.params["λ"] = None
        for gamma_str in self.params["π"]:
            for level_value in self.params["π"][gamma_str]["prob_dist_match"].values():
                level_value["probability"] = None
            for level_value in self.params["π"][gamma_str][
                "prob_dist_non_match"
            ].values():
                level_value["probability"] = None

    def save_params_to_iteration_history(self):
        """
        Take current params and
        """
        current_params = copy.deepcopy(self.params)
        self.param_history.append(current_params)
        if "log_likelihood" in self.params:
            self.log_likelihood_exists = True

    def populate_params(self, lambda_value, pi_df_collected):
        """
        Take results of sql query that computes updated values
        and update parameters
        """

        self.params["λ"] = lambda_value

        # Populate all values with 0 (we sometimes never see some values of gamma so everything breaks.)
        for gamma_str in self.params["π"]:
            for level_key, level_value in self.params["π"][gamma_str][
                "prob_dist_match"
            ].items():
                level_value["probability"] = 0
            for level_key, level_value in self.params["π"][gamma_str][
                "prob_dist_non_match"
            ].items():
                level_value["probability"] = 0

        for row_dict in pi_df_collected:
            gamma_str = row_dict["gamma_col"]
            level_int = row_dict["gamma_value"]
            match_prob = row_dict["new_probability_match"]
            non_match_prob = row_dict["new_probability_non_match"]
            if level_int != -1:
                self.set_pi_value(gamma_str, level_int, "match", match_prob)
                self.set_pi_value(gamma_str, level_int, "non_match", non_match_prob)

    def update_params(self, lambda_value, pi_df_collected):
        """
        Save current value of parameters to iteration history
        Reset values
        Then update the parameters from the dataframe
        """
        self.save_params_to_iteration_history()
        self.reset_param_values_to_none()
        self.populate_params(lambda_value, pi_df_collected)
        self.iteration += 1

    ### The rest of this module is just 'presentational' elements - charts, and __repr__ etc.

    def pi_iteration_chart(self):  # pragma: no cover

        if self.real_params:
            data = self.iteration_history_df_gammas()
            data_real = self.convert_params_dict_to_data(self.real_params, "real_param")
            data.extend(data_real)
        else:
            data = self.iteration_history_df_gammas()

        pi_iteration_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(pi_iteration_chart_def)
        else:
            return pi_iteration_chart_def

    def lambda_iteration_chart(self):  # pragma: no cover
        data = self.iteration_history_df_lambdas()
        if self.real_params:
            data.append({"λ": self.real_params["λ"], "iteration": "real_param"})

        lambda_iteration_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(lambda_iteration_chart_def)
        else:
            return lambda_iteration_chart_def

    def ll_iteration_chart(self):  # pragma: no cover
        if self.log_likelihood_exists:
            data = self.iteration_history_df_log_likelihood()

            ll_iteration_chart_def["data"]["values"] = data

            if altair_installed:
                return alt.Chart.from_dict(ll_iteration_chart_def)
            else:
                return ll_iteration_chart_def
        else:
            raise Exception(
                "Log likelihood not calculated.  To calculate pass 'compute_ll=True' to iterate(). Note this causes algorithm to run more slowly because additional calculations are required."
            )

    def probability_distribution_chart(self):  # pragma: no cover
        """
        If altair is installed, returns the chart
        Otherwise will return the chart spec as a dictionary
        """
        data = self.convert_params_dict_to_data(self.params)

        probability_distribution_chart["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(probability_distribution_chart)
        else:
            return probability_distribution_chart

    def all_charts_write_html_file(self, filename="sparklink_charts.html"):

        if altair_installed:
            c1 = self.probability_distribution_chart().to_json(indent=None)
            c2 = self.lambda_iteration_chart().to_json(indent=None)
            c3 = self.pi_iteration_chart().to_json(indent=None)
            if self.log_likelihood_exists:
                c4 = self.ll_iteration_chart().to_json(indent=None)
            else:
                c4 = ""

            with open(filename, "w") as f:
                f.write(
                    multi_chart_template.format(
                        vega_version=alt.VEGA_VERSION,
                        vegalite_version=alt.VEGALITE_VERSION,
                        vegaembed_version=alt.VEGAEMBED_VERSION,
                        spec1=c1,
                        spec2=c2,
                        spec3=c3,
                        spec4=c4,
                    )
                )

    def __repr__(self):  # pragma: no cover

        p = self.params
        lines = []
        lines.append(f"λ (proportion of matches) = {p['λ']}")

        for gamma_str, gamma_dict in p["π"].items():
            lines.append("------------------------------------")
            lines.append(f"{gamma_str}: {gamma_dict['desc']}")
            lines.append("")

            last_level = list(gamma_dict["prob_dist_non_match"].keys())[-1]

            lines.append(f"Probability distribution of gamma values amongst matches:")
            for level_key, level_value in gamma_dict["prob_dist_match"].items():

                value = level_value["value"]

                value_label = ""
                if value == 0:
                    value_label = (
                        "(level represents lowest category of string similarity)"
                    )

                if level_key == last_level:
                    value_label = (
                        "(level represents highest category of string similarity)"
                    )

                prob = level_value["probability"]
                if not prob:
                    prob = "None"
                else:
                    prob = f"{prob:4f}"

                lines.append(f"    value {value}: {prob} {value_label}")
            lines.append("")

            lines.append(
                f"Probability distribution of gamma values amongst non-matches:"
            )
            for level_key, level_value in gamma_dict["prob_dist_non_match"].items():

                value = level_value["value"]

                value_label = ""
                if value == 0:
                    value_label = (
                        "(level represents lowest category of string similarity)"
                    )

                if level_key == last_level:
                    value_label = (
                        "(level represents highest category of string similarity)"
                    )

                prob = level_value["probability"]
                if not prob:
                    prob = "None"
                else:
                    prob = f"{prob:4f}"

                lines.append(f"    value {value}: {prob} {value_label}")

        return "\n".join(lines)

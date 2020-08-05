import copy
import os
import json


try:
    from pyspark.sql.session import SparkSession
except ImportError:
    SparkSession = None


from .gammas import complete_settings_dict
from .validate import _get_default_value
from .chart_definitions import (
    lambda_iteration_chart_def,
    probability_distribution_chart,
    gamma_distribution_chart_def,
    ll_iteration_chart_def,
    bayes_factor_chart_def,
    bayes_factor_history_chart_def,
    multi_chart_template,
)
from .check_types import check_types
import random
import warnings

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False

import logging

logger = logging.getLogger(__name__)


class Params:
    """Stores the current model parameters (in self.params) and values for params for all previous iterations

    Attributes:
        params (str): A dictionary storing the current parameters.

    """

    def __init__(self, settings:dict, spark:SparkSession):
        """[summary]

        Args:
            settings (dict): A splink setting object
            spark (SparkSession): Your sparksession. Defaults to None.
        """

        self.param_history = []

        self.iteration = 1

        self.settings = complete_settings_dict(settings, spark)
        self.params = {"λ": settings["proportion_of_matches"], "π": {}}

        self.log_likelihood_exists = False

        self.real_params = None

        self._generate_param_dict()

    @property
    def _gamma_cols(self):
        return self.params["π"].keys()

    def describe_gammas(self):
        return {k: i["desc"] for k, i in self.params["π"].items()}

    def _generate_param_dict(self):
        """Uses the splink settings object to generate a parameter dictionary
        """

        for col_dict in self.settings["comparison_columns"]:
            if "col_name" in col_dict:
                col_name = col_dict["col_name"]
            elif "custom_name" in col_dict:
                col_name = col_dict["custom_name"]

            i = col_dict["gamma_index"]

            self.params["π"][f"gamma_{col_name}"] = {}
            self.params["π"][f"gamma_{col_name}"]["gamma_index"] = i

            self.params["π"][f"gamma_{col_name}"]["desc"] = f"Comparison of {col_name}"
            self.params["π"][f"gamma_{col_name}"]["column_name"] = f"{col_name}"

            if "custom_name" in col_dict:
                self.params["π"][f"gamma_{col_name}"]["custom_comparison"] = True
                self.params["π"][f"gamma_{col_name}"]["custom_columns_used"] = col_dict["custom_columns_used"]
            else:
                self.params["π"][f"gamma_{col_name}"]["custom_comparison"] = False

            num_levels = col_dict["num_levels"]
            self.params["π"][f"gamma_{col_name}"]["num_levels"] = num_levels

            prob_dist_match = {}
            prob_dist_non_match = {}

            probs_m = col_dict["m_probabilities"]
            probs_nm = col_dict["u_probabilities"]

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

            self.params["π"][f"gamma_{col_name}"]["prob_dist_match"] = prob_dist_match
            self.params["π"][f"gamma_{col_name}"]["prob_dist_non_match"] = prob_dist_non_match

    def _set_pi_value(self, gamma_str, level_int, match_str, prob_float):
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
    def _convert_params_dict_to_dataframe(params, iteration_num=None):
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

    def _convert_params_dict_to_bayes_factor_data(self):
        """
        Get the data needed for a chart that shows which comparison
        vector values have the greatest effect on match probability
        """
        data = []
        # Want to compare the u and m probabilities
        lam = self.params['λ']
        pi = gk = self.params["π"]
        gk = list(pi.keys())

        for g in gk:
            this_gamma = pi[g]
            for l in range(this_gamma["num_levels"]):
                row = {}
                level = f"level_{l}"
                row["level"] = l
                row["column"] = this_gamma["column_name"]
                row["m"] = this_gamma["prob_dist_match"][level]["probability"]
                row["u"] = this_gamma["prob_dist_non_match"][level]["probability"]
                row["level_proportion"] = row["m"]*lam + row["u"]*(1-lam)
                try:
                    row["bayes_factor"] = row["m"] / row["u"]
                except ZeroDivisionError:
                    row["bayes_factor"] = None

                data.append(row)
        return data
    
    def _convert_params_dict_to_bayes_factor_iteration_history(self):
        """
        Get the data needed for a chart that shows which comparison
        vector values have the greatest effect on match probability
        """
        data = []

        pi = gk = self.params["π"]
        gk = list(pi.keys())

        for it_num, param_value in enumerate(self.param_history):
            for g in gk:
                pi = gk = self.param_history[it_num]["π"]
                gk = list(pi.keys())
                this_gamma = pi[g]
                for l in range(this_gamma["num_levels"]):
                    row = {}
                    row["iteration"] = it_num
                    level = f"level_{l}"
                    row["level"] = l
                    row["num_levels"] = this_gamma["num_levels"]
                    row["column"] = this_gamma["column_name"]
                    row["m"] = this_gamma["prob_dist_match"][level]["probability"]
                    row["u"] = this_gamma["prob_dist_non_match"][level]["probability"]
                    try:
                        row["bayes_factor"] = row["m"] / row["u"]
                    except ZeroDivisionError: 
                        row["bayes_factor"] = None
                    
                    if it_num == len(self.param_history)-1:
                        row["final"]=True
                    else:
                        row["final"]=False

                    data.append(row)
        return data

    def _iteration_history_df_gammas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.extend(self._convert_params_dict_to_dataframe(param_value, it_num))
        data.extend(self._convert_params_dict_to_dataframe(self.params, it_num + 1))
        return data

    def _iteration_history_df_lambdas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append({"λ": param_value["λ"], "iteration": it_num})
        data.append({"λ": self.params["λ"], "iteration": it_num + 1})
        return data

    def _iteration_history_df_log_likelihood(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append(
                {"log_likelihood": param_value["log_likelihood"], "iteration": it_num}
            )
        data.append(
            {"log_likelihood": self.params["log_likelihood"], "iteration": it_num + 1}
        )
        return data

    def _reset_param_values_to_none(self):
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

    def _save_params_to_iteration_history(self):
        """
        Take current params and
        """
        current_params = copy.deepcopy(self.params)
        self.param_history.append(current_params)
        if "log_likelihood" in self.params:
            self.log_likelihood_exists = True

    def _populate_params(self, lambda_value, pi_df_collected):
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
                self._set_pi_value(gamma_str, level_int, "match", match_prob)
                self._set_pi_value(gamma_str, level_int, "non_match", non_match_prob)

    def _update_params(self, lambda_value, pi_df_collected):
        """
        Save current value of parameters to iteration history
        Reset values
        Then update the parameters from the dataframe
        """
        self._save_params_to_iteration_history()
        self._reset_param_values_to_none()
        self._populate_params(lambda_value, pi_df_collected)
        self.iteration += 1

    def _to_dict(self):
        p_dict = {}
        p_dict["current_params"] = self.params
        p_dict["historical_params"] = self.param_history
        p_dict["settings"] = self.settings

        return p_dict

    def save_params_to_json_file(self, path=None, overwrite=False):

        proceed_with_write = False
        if not path:
            raise ValueError("Must provide a path to write to")

        if os.path.isfile(path):
            if overwrite:
                proceed_with_write = True
            else:
                raise ValueError(
                    f"The path {path} already exists. Please provide a different path."
                )
        else:
            proceed_with_write = True

        if proceed_with_write:
            d = self._to_dict()
            with open(path, "w") as f:
                json.dump(d, f, indent=4)

    def is_converged(self):
        p_latest = self.params
        p_previous = self.param_history[-1]
        threshold = self.settings["em_convergence"]

        p_new = {key:value for key, value in _flatten_dict(p_latest).items() if '_probability' in key.lower()}
        p_old = {key:value for key, value in _flatten_dict(p_previous).items() if '_probability' in key.lower()}

        diff = [abs(p_new[item] - p_old[item]) < threshold for item in p_new]

        biggest_change = 0
        biggest_change_key = ''
        for key in p_new.keys():
            new_change = abs(p_new[key] - p_old[key])
            if new_change > biggest_change:
                biggest_change = new_change
                biggest_change_key = key
        
        logger.info(f"The maximum change in parameters was {biggest_change} for key {biggest_change_key}")

        return(all(diff))
    
    def get_settings_with_current_params(self):
        return get_or_update_settings(self)

    ### The rest of this module is just 'presentational' elements - charts, and __repr__ etc.

    def _print_m_u_probs(self):

        def field_value_to_probs(fv):
            m_probs = []
            for key, val in fv['prob_dist_match'].items():
                m_probs.append(val["probability"])
            u_probs = []
            for key, val in fv['prob_dist_non_match'].items():
                u_probs.append(val["probability"])
            
            print(f'"m_probabilities": {m_probs},')
            print(f'"u_probabilities": {u_probs}')
                
        for field, value in self.params['π'].items():
            print(field)
            field_value_to_probs(value)

    def lambda_iteration_chart(self):  # pragma: no cover
        data = self._iteration_history_df_lambdas()
        if self.real_params:
            data.append({"λ": self.real_params["λ"], "iteration": "real_param"})

        lambda_iteration_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(lambda_iteration_chart_def)
        else:
            return lambda_iteration_chart_def

    def ll_iteration_chart(self):  # pragma: no cover
        if self.log_likelihood_exists:
            data = self._iteration_history_df_log_likelihood()

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
        data = self._convert_params_dict_to_dataframe(self.params)

        probability_distribution_chart["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(probability_distribution_chart)
        else:
            return probability_distribution_chart
        
    def gamma_distribution_chart(self):  # pragma: no cover
        """
        If altair is installed, returns the chart
        Otherwise will return the chart spec as a dictionary
        """
        data = self._convert_params_dict_to_bayes_factor_data()

        gamma_distribution_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(gamma_distribution_chart_def)
        else:
            return gamma_distribution_chart_def

    def bayes_factor_chart(self):  # pragma: no cover
        """
        If altair is installed, returns the chart
        Otherwise will return the chart spec as a dictionary
        """
        data = self._convert_params_dict_to_bayes_factor_data()

        bayes_factor_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(bayes_factor_chart_def)
        else:
            return bayes_factor_chart_def
        
    def bayes_factor_history_charts(self):
        """
        If altair is installed, returns the chart
        Otherwise will return the chart spec as a dictionary
        """
        # Empty list of chart definitions
        chart_defs = []
    
        # Full iteration history
        data = self._convert_params_dict_to_bayes_factor_iteration_history()
    
        # Create charts for each column
        for col_dict in self.settings["comparison_columns"]:
        
            # Get column name
            if "col_name" in col_dict:
                col_name = col_dict["col_name"]
            elif "custom_name" in col_dict:
                col_name = col_dict["custom_name"] 
           
            chart_def = copy.deepcopy(bayes_factor_history_chart_def)
            # Assign iteration history to values of chart_def
            chart_def["data"]["values"] = [d for d in data if d['column']==col_name]
            chart_def["title"]["text"] = col_name
            chart_def["hconcat"][1]["layer"][0]["encoding"]["color"]["legend"]["tickCount"] = col_dict["num_levels"]-1
            chart_defs.append(chart_def)
        
        combined_charts = {
            "config": {
                "view": {"width": 400, "height": 120},
            },
            "title": {"text":"Bayes factor iteration history", "anchor": "middle"},
            "vconcat": chart_defs,
            "resolve": {"scale":{"color": "independent"}},
            '$schema': 'https://vega.github.io/schema/vega-lite/v4.8.1.json'
        }
        
        if altair_installed:
            return alt.Chart.from_dict(combined_charts)
        else:
            return combined_charts
        

    def all_charts_write_html_file(self, filename="splink_charts.html", overwrite=False):

        if os.path.isfile(filename):
            if not overwrite:
                raise ValueError(
                    f"The path {filename} already exists. Please provide a different path."
                )

        if altair_installed:
            c1 = self.probability_distribution_chart().to_json(indent=None)
            c2 = self.bayes_factor_chart().to_json(indent=None)
            c3 = self.lambda_iteration_chart().to_json(indent=None)

            if self.log_likelihood_exists:
                c4 = self.ll_iteration_chart().to_json(indent=None)
            else:
                c4 = ""

            c5 = self.bayes_factor_history_charts().to_json(indent=None)
            c6 = self.gamma_distribution_chart().to_json(indent=None)
            
            with open(filename, "w") as f:
                f.write(
                    multi_chart_template.format(
                        vega_version=alt.VEGA_VERSION,
                        vegalite_version=alt.VEGALITE_VERSION,
                        vegaembed_version=alt.VEGAEMBED_VERSION,
                        spec1=c1,
                        spec2=c6,
                        spec3=c2,
                        spec4=c3,
                        spec5=c4,
                        spec6=c5
                    )
                )
        else:
            c1 = json.dumps(self.probability_distribution_chart())
            c2 = json.dumps(self.bayes_factor_chart())
            c3 = json.dumps(self.lambda_iteration_chart())

            if self.log_likelihood_exists:
                c4 = json.dumps(self.ll_iteration_chart())
            else:
                c4 = ""
            
            c5 = json.dumps(self.bayes_factor_history_charts())
            c6 = json.dumps(self.gamma_distribution_chart())
            
            with open(filename, "w") as f:
                f.write(
                    multi_chart_template.format(
                        vega_version="5",
                        vegalite_version="3.3.0",
                        vegaembed_version="4",
                        spec1=c1,
                        spec2=c6,
                        spec3=c2,
                        spec4=c3,
                        spec5=c4,
                        spec6=c5
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


def load_params_from_json(path):
    # Load params
    with open(path, "r") as f:
        params_from_json = json.load(f)

    p = load_params_from_dict(params_from_json)

    return p


def load_params_from_dict(param_dict):

    keys = set(param_dict.keys())
    expected_keys = {"current_params", "settings", "historical_params"}

    if keys == expected_keys:
        p = Params(settings=param_dict["settings"], spark=None)


        p.params = param_dict["current_params"]
        p.param_history = param_dict["historical_params"]
    else:
        raise ValueError("Your saved params seem to be corrupted")

    return p


def _flatten_dict(dictionary, accumulator=None, parent_key=None, separator="_"):
    if accumulator is None:
        accumulator = {}
    for k, v in dictionary.items():
        k = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            _flatten_dict(dictionary=v, accumulator=accumulator, parent_key=k)
            continue
        accumulator[k] = v
    return accumulator

@check_types
def get_or_update_settings(params: Params, settings: dict = None):
    
    if not settings:
        settings = params.settings
    
    settings["proportion_of_matches"] = params.params['λ']                     
                         
    for comp in settings["comparison_columns"]:
        if "col_name" in comp.keys():
            label = "gamma_"+comp["col_name"]
        else:
            label = "gamma_"+comp["custom_name"]
            
        if "num_levels" in comp.keys():
            num_levels = comp["num_levels"]
        else:
            num_levels = _get_default_value("num_levels", is_column_setting=True)
        
        
        if label in params.params["π"].keys():
            saved = params.params["π"][label]
    
            if num_levels == saved["num_levels"]:
                m_probs = [val['probability'] for key, val in saved["prob_dist_match"].items()]
                u_probs = [val['probability'] for key, val in saved["prob_dist_non_match"].items()]
    
                comp["m_probabilities"] = m_probs
                comp["u_probabilities"] = u_probs
            else:
                warnings.warn(f"{label}: Saved m and u probabilities do not match the specified number of levels ({num_levels}) - default probabilities will be used")
    
    return(settings)

from copy import deepcopy
import os
import json

from pyspark.sql.session import SparkSession

from .settings import Settings, complete_settings_dict
from .validate import _get_default_value

# from .chart_definitions import (
#     lambda_iteration_chart_def,
#     probability_distribution_chart,
#     gamma_distribution_chart_def,
#     ll_iteration_chart_def,
#     bayes_factor_chart_def,
#     bayes_factor_history_chart_def,
#     multi_chart_template,
#     pi_iteration_chart_def,
# )
from .check_types import check_types
import warnings

altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False

import logging

logger = logging.getLogger(__name__)


def _settings_to_dataframe(settings: Settings, iteration: int = None):
    """
    Convert the params dict into a dataframe.


    [{'gamma': 'gamma_first_name',
    'match': 1,
    'value_of_gamma': 'level_0',
    'probability': 0.1,
    'value': 0,
    'column': 'first_name',
    'iteration': },
    {}]
    """

    return settings.as_rows()


class Params:
    """Stores the current model parameters (in self.params) and values for params for all previous iterations

    Attributes:
        params (str): A dictionary storing the current parameters.

    """

    def __init__(self, settings: dict, spark: SparkSession):
        """

        Args:
            settings (dict): A splink setting dictionary
            spark (SparkSession): Your sparksession. Defaults to None.
        """

        self.param_history = []

        self.iteration = 0

        # Settings is just a wrapper around the settings dict
        settings_dict_original = deepcopy(settings)
        self.settings_original = Settings(settings_dict_original)

        settings_dict_completed = complete_settings_dict(deepcopy(settings), spark)

        self.params = Settings(deepcopy(settings_dict_completed))

        self.log_likelihood_exists = False

    def _populate_params_from_maximisation_step(self, lambda_value, pi_df_collected):
        """
        Take results of sql query that computes updated values
        and update parameters.

        df_pi_collected is like
        gamma_value, new_probability_match, new_probability_non_match, gamma_col
        """

        self.params.reset_all_probabilities()

        self.params["proportion_of_matches"] = lambda_value
        for row_dict in pi_df_collected:
            name = row_dict["column_name"]  # gamma_col is the nam
            level_int = row_dict["gamma_value"]
            match_prob = row_dict["new_probability_match"]
            non_match_prob = row_dict["new_probability_non_match"]

            self.params.set_m_probability(name, level_int, match_prob, force=False)
            self.params.set_u_probability(name, level_int, non_match_prob, force=False)

    def is_converged(self):
        p_latest = self.params
        p_previous = self.param_history[-1]
        threshold = self.params["em_convergence"]

        diffs = []

        change_lambda = abs(
            p_latest["proportion_of_matches"] - p_previous["proportion_of_matches"]
        )
        diffs.append(
            {"col_name": "proportion_of_matches", "diff": change_lambda, "level": ""}
        )

        compare = zip(p_latest.comparison_columns, p_previous.comparison_columns)
        for c_latest, c_previous in compare:
            for m_or_u in ["m_probabilities", "u_probabilities"]:
                for gamma_index in range(c_latest.num_levels):
                    val_latest = c_latest[m_or_u][gamma_index]
                    val_previous = c_previous[m_or_u][gamma_index]
                    diff = abs(val_latest - val_previous)
                    diffs.append(
                        {"col_name": c_latest.name, "diff": diff, "level": gamma_index}
                    )

        diffs = sorted(diffs, key=lambda x: x["diff"], reverse=True)
        largest_diff = diffs[0]["diff"]
        largest_diff_name = diffs[0]["col_name"]
        largest_diff_level = diffs[0]["level"]

        if largest_diff_level != "":
            level_info = f", level {largest_diff_level}"
        else:
            level_info = ""
        logger.info(
            f"The maximum change in parameters was {largest_diff} for key {largest_diff_name}{level_info}"
        )

        return largest_diff < threshold

    def save_params_to_iteration_history(self):
        """
        Take current params and
        """
        current_params = deepcopy(self.params.settings_dict)

        self.param_history.append(Settings(current_params))
        if "log_likelihood" in self.params.settings_dict:
            self.log_likelihood_exists = True

    def _to_dict(self):
        p_dict = {}
        p_dict["current_params"] = self.params.settings_dict
        p_dict["historical_params"] = [s.settings_dict for s in self.param_history]
        p_dict["settings_original"] = self.settings_original.settings_dict

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

    #######################################################################################
    # The rest of this module is just 'presentational' elements - charts, and __repr__ etc.
    #######################################################################################

    def _convert_params_dict_to_bayes_factor_data(self):
        pass

    def _convert_params_dict_to_bayes_factor_iteration_history(self):
        """
        Get the data needed for a chart that shows which comparison
        vector values have the greatest effect on match probability
        """
        pass

    def _iteration_history_df_gammas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.extend(self._convert_params_dict_to_dataframe(param_value, it_num))

        return data

    def _iteration_history_df_lambdas(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append({"λ": param_value["λ"], "iteration": it_num})

        return data

    def _iteration_history_df_log_likelihood(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.append(
                {"log_likelihood": param_value["log_likelihood"], "iteration": it_num}
            )

        return data

    def lambda_iteration_chart(self):  # pragma: no cover
        data = self._iteration_history_df_lambdas()

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
            chart_def["data"]["values"] = [d for d in data if d["column"] == col_name]
            chart_def["title"]["text"] = col_name
            chart_def["hconcat"][1]["layer"][0]["encoding"]["color"]["legend"][
                "tickCount"
            ] = (col_dict["num_levels"] - 1)
            chart_defs.append(chart_def)

        combined_charts = {
            "config": {
                "view": {"width": 400, "height": 120},
            },
            "title": {"text": "Bayes factor iteration history", "anchor": "middle"},
            "vconcat": chart_defs,
            "resolve": {"scale": {"color": "independent"}},
            "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
        }

        if altair_installed:
            return alt.Chart.from_dict(combined_charts)
        else:
            return combined_charts

    def pi_iteration_chart(self):  # pragma: no cover

        data = self._iteration_history_df_gammas()

        pi_iteration_chart_def["data"]["values"] = data

        if altair_installed:
            return alt.Chart.from_dict(pi_iteration_chart_def)
        else:
            return pi_iteration_chart_def

    def all_charts_write_html_file(
        self, filename="splink_charts.html", overwrite=False
    ):

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
                        spec6=c5,
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
                        vega_version="5.17.0",
                        vegalite_version="4.17.0",
                        vegaembed_version="6",
                        spec1=c1,
                        spec2=c6,
                        spec3=c2,
                        spec4=c3,
                        spec5=c4,
                        spec6=c5,
                    )
                )

    def __repr__(self):  # pragma: no cover

        p = self.params
        lines = []
        lines.append(f"λ (proportion of matches) = {p['proportion_of_matches']}")

        previous_col = ""
        for row in p.m_u_as_rows():
            if row["column"] != previous_col:
                lines.append("------------------------------------")
                lines.append(f"Comparison of {row['column']}")
                lines.append("")
            previous_col = row["column"]

            lines.append(f"{row['value_of_gamma']}")
            lines.append(
                f"   Prob amongst matches:     {row['m_probability']*100:.3g}%"
            )
            lines.append(
                f"   Prob amongst non-matches: {row['u_probability']*100:.3g}%"
            )
            lines.append(f"   Bayes factor:             {row['bayes_factor']:,.3g}")

        return "\n".join(lines)


def load_params_from_json(path):
    # Load params
    with open(path, "r") as f:
        params_from_json = json.load(f)

    p = load_params_from_dict(params_from_json)

    return p


def load_params_from_dict(param_dict):

    keys = set(param_dict.keys())

    expected_keys = {
        "current_params",
        "settings_original",
        "historical_params",
    }

    if keys == expected_keys:
        p = Params(param_dict["current_params"], spark=None)

        p.param_history = [Settings(p) for p in param_dict["historical_params"]]
        p.settings_original = Settings(param_dict["settings_original"])
    else:
        raise ValueError("Your saved params seem to be corrupted")

    return p

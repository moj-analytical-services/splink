import copy
import json
from pprint import pprint
from gammas import complete_settings_dict
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

    def __init__(self, gamma_settings, starting_lambda = 0.5):
        self.params = {'λ': starting_lambda,
                       'π': {}}

        self.param_history = []

        self.iteration = 1

        self.gamma_settings = complete_settings_dict(gamma_settings)

        self.generate_param_dict()

    @property
    def gamma_cols(self):
        return self.params['π'].keys()

    def describe_gammas(self):
        return {k:i["desc"]  for k, i in self.params['π'].items()}

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


            probs_m = [random.uniform(0, 1) for r in range(num_levels)]
            s = sum(probs_m)
            probs_m = [p/s for p in probs_m]

            probs_nm = [random.uniform(0, 1) for r in range(num_levels)]
            s = sum(probs_nm)
            probs_nm = [p/s for p in probs_nm]


            for level_num in range(num_levels):
                prob_dist_match[f"level_{level_num}"] = {
                    "value": level_num, "probability": probs_m[level_num]}
                prob_dist_non_match[f"level_{level_num}"] = {
                    "value": level_num, "probability": probs_nm[level_num]}

            self.params["π"][f"gamma_{i}"]["prob_dist_match"] = prob_dist_match
            self.params["π"][f"gamma_{i}"]["prob_dist_non_match"] = prob_dist_non_match

    def update_pi_value(self, gamma_str, level_int, match_str, prob_float):
        """
        gamma_str e.g. gamma_0
        level_int e.g. 1
        match_str e.g. match or non_match
        prob_float e.g. 0.5

        """
        this_g = self.params["π"][gamma_str]
        this_g[f"prob_dist_{match_str}"][f"level_{level_int}"]["probability"] = prob_float


    @staticmethod
    def convert_params_dict_to_data(params, iteration_num=None):
        """
        Convert the params dict into a dataframe

        If iteration_num is specified, this will be turned into a column in the dataframe
        """

        data = []
        for gamma_str, gamma_dict in params['π'].items():

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

    def iteration_history_df(self):
        data = []
        for it_num, param_value in enumerate(self.param_history):
            data.extend(self.convert_params_dict_to_data(param_value, it_num))
        data.extend(self.convert_params_dict_to_data(self.params, it_num+1))
        return data

    def reset_param_values_to_none(self):
        """
        Reset λ and all probability values to None to ensure we
        don't accidentally re-use old values
        """
        self.params["λ"] = None
        for gamma_str in self.params['π']:
            for level_key, level_value in self.params["π"][gamma_str]["prob_dist_match"].items():
                level_value["probability"] = None
            for level_key, level_value in self.params["π"][gamma_str]["prob_dist_non_match"].items():
                level_value["probability"] = None

    def save_params_to_iteration_history(self):
        """
        Take current params and
        """
        current_params = copy.deepcopy(self.params)
        self.param_history.append(current_params)

    def populate_params(self, lambda_value, pi_df_collected):
        """
        Take results of sql query that computes updated values
        and update parameters
        """

        self.params["λ"] = lambda_value

        for row in pi_df_collected:
            row_dict = row.asDict()
            gamma_str = row_dict["gamma_col"]
            level_int = row_dict["gamma_value"]
            match_prob = row_dict["new_probability_match"]
            non_match_prob = row_dict["new_probability_non_match"]

            self.update_pi_value(gamma_str, level_int,"match",match_prob)
            self.update_pi_value(gamma_str, level_int, "non_match", non_match_prob)



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

    def probability_distribution_chart(self):
        """
        If altair is installed, returns the chart
        Otherwise will return the chart spec as a dictionary
        """
        data = self.convert_params_dict_to_data(self.params)


        # chart_data = alt.Data(values=data)
        # chart = alt.Chart(chart_data).mark_bar().encode(
        #     x='probability:Q',
        #     y=alt.Y('value:N', axis=alt.Axis(title='𝛾 value')),
        #     color='match:N',
        #     row=alt.Row('column:N', sort=alt.SortField("gamma"))
        # ).resolve_scale(
        #     y='independent'
        # ).properties(width=150)


        # c0 = chart.transform_filter(
        #     (datum.match == 0)
        # )

        # c1 = chart.transform_filter(
        #     (datum.match == 1)
        # )

        # facetted_chart = c0 | c1

        # fc = facetted_chart.configure_title(
        #     anchor='middle'
        # ).properties(
        #     title='Probability distribution of comparison vector values, m=0 and m=1'
        # )

        chart_def = {'config': {'view': {'width': 400, 'height': 300},
                                'mark': {'tooltip': None},
                                'title': {'anchor': 'middle'}},
                     'hconcat': [{'mark': 'bar',
                                  'encoding': {'color': {'type': 'nominal', 'field': 'match'},
                                               'row': {'type': 'nominal', 'field': 'column', 'sort': {'field': 'gamma'}},
                                               'x': {'type': 'quantitative', 'field': 'probability'},
                                               'y': {'type': 'nominal', 'axis': {'title': '𝛾 value'}, 'field': 'value'}},
                                  'resolve': {'scale': {'y': 'independent'}},
                                  'transform': [{'filter': '(datum.match === 0)'}],
                                  'width': 150},
                                 {'mark': 'bar',
                                  'encoding': {'color': {'type': 'nominal', 'field': 'match'},
                                               'row': {'type': 'nominal', 'field': 'column', 'sort': {'field': 'gamma'}},
                                               'x': {'type': 'quantitative', 'field': 'probability'},
                                               'y': {'type': 'nominal', 'axis': {'title': '𝛾 value'}, 'field': 'value'}},
                                  'resolve': {'scale': {'y': 'independent'}},
                                  'transform': [{'filter': '(datum.match === 1)'}],
                                  'width': 150}],
                     'data': {'values': data},
                     'title': 'Probability distribution of comparison vector values, m=0 and m=1',
                     '$schema': 'https://vega.github.io/schema/vega-lite/v3.4.0.json'}
        if altair_installed:
            return alt.Chart.from_dict(chart_def)
        else:
            return chart_def

    def __repr__(self):

        p = self.params
        lines = []
        lines.append(f"λ (proportion of matches) = {p['λ']}")

        for gamma_str, gamma_dict in p['π'].items():
            lines.append("------------------------------------")
            lines.append(f"{gamma_str}: {gamma_dict['desc']}")
            lines.append("")

            last_level = list(gamma_dict["prob_dist_non_match"].keys())[-1]

            lines.append(
                f"Probability distribution of gamma values amongst matches:")
            for level_key, level_value in gamma_dict["prob_dist_match"].items():

                value = level_value["value"]

                value_label = ""
                if value == 0:
                    value_label = "(level represents lowest category of string similarity)"

                if level_key == last_level:
                    value_label = "(level represents highest category of string similarity)"

                prob = level_value["probability"]
                if not prob:
                    prob = "None"
                else:
                    prob = f"{prob:4f}"

                lines.append(f"    value {value}: {prob} {value_label}")
            lines.append("")

            lines.append(
                f"Probability distribution of gamma values amongst non-matches:")
            for level_key, level_value in gamma_dict["prob_dist_non_match"].items():

                value = level_value["value"]

                value_label = ""
                if value == 0:
                    value_label = "(level represents lowest category of string similarity)"

                if level_key == last_level:
                    value_label = "(level represents highest category of string similarity)"

                prob = level_value["probability"]
                if not prob:
                    prob = "None"
                else:
                    prob = f"{prob:4f}"

                lines.append(f"    value {value}: {prob} {value_label}")

        return "\n".join(lines)

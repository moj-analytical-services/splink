import copy
import json
from pprint import pprint
import altair as alt


class Params:
    """
    Stores both current model parameters (in self.params)
    and values of params for all previous iterations
    of the model (in self.param_history)
    """

    def __init__(self, ):
        self.params = {'λ': None,
                       'π': {}}

        self.param_history = []

        self.iteration = 1

    def generate_param_dict(self, col_names, levels_dict):
        """
        e.g col_names  = ["first_name", "surname", "dob"]

        levels_dict = {"first_name" : 3, "surname": 3}
        """

        for i, col in enumerate(col_names):
            self.params["π"][f"gamma_{i}"] = {}
            self.params["π"][f"gamma_{i}"]["desc"] = f"Match on {col}"
            self.params["π"][f"gamma_{i}"]["column"] = f"{col}"

            if col in levels_dict:
                num_levels = levels_dict[col]
            else:
                num_levels = 2

            prob_dist_match = {}
            prob_dist_non_match = {}
            for level_num in range(num_levels):
                prob_dist_match[f"level_{level_num}"] = {
                    "value": level_num, "probability": 1/num_levels}
                prob_dist_non_match[f"level_{level_num}"] = {
                    "value": level_num, "probability": 1/num_levels}

            self.params["π"][f"gamma_{i}"]["prob_dist_match"] = prob_dist_match
            self.params["π"][f"gamma_{i}"]["prob_dist_non_match"] = prob_dist_non_match

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
                if iteration_num:
                    this_row["iteration"] = iteration_num
                this_row["gamma"] = gamma_str
                this_row["match"] = 1
                this_row["value_of_gamma"] = level_str
                this_row["probability"] = level_dict["probability"]
                this_row["value"] = level_dict["value"]
                this_row["column"] = gamma_dict["column"]
                data.append(this_row)

            for level_str, level_dict in gamma_dict["prob_dist_non_match"].items():
                this_row = {}
                if iteration_num:
                    this_row["iteration"] = iteration_num
                this_row["gamma"] = gamma_str
                this_row["match"] = 0
                this_row["value_of_gamma"] = level_str
                this_row["probability"] = level_dict["probability"]
                this_row["value"] = level_dict["value"]
                this_row["column"] = gamma_dict["column"]
                data.append(this_row)
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
        param_history.push(current_params)

    def populate_params(self, collected_dataframe):
        """
        Take results of sql query that computes updated values
        and update parameters
        """
        pass

    def update_params(self, collected_dataframe):
        """
        Save current value of parameters to iteration history
        Reset values
        Then update the parameters from the dataframe
        """
        self.save_params_to_iteration_history()
        self.reset_param_values_to_none()
        self.populate_params()

    def probability_distribution_chart(self):
        """
        To plot:

        chart_def = p.probability_distribution_chart()
        alt.Chart.from_dict(chart_def)

        """
        data = self.convert_params_dict_to_data(self.params)

        # chart_data = alt.Data(values=data)
        # alt.Chart(chart_data).mark_bar().encode(
        #     x='probability:Q',
        #     y= alt.Y('value:N', axis=alt.Axis(title='𝛾 value')),
        #     color = 'match:N',
        #     row = 'column:N',
        #     column = alt.Column('match:O', header=alt.Header(title = ""))
        # ).resolve_scale(
        #     y='independent'
        # ).properties(title='Probability distributions of comparison vector values',
        #     width="200"
        # ).configure_title(
        #     fontSize=20,
        #     anchor='middle'
        # )

        return {'config': {'view': {'width': 200, 'height': 300}, 'mark': {'tooltip': None}, 'title': {'anchor': 'middle', 'fontSize': 20}}, 'data': {'values': data}, 'mark': 'bar', 'encoding': {'color': {'type': 'nominal', 'field': 'match'}, 'column': {'type': 'ordinal', 'field': 'match', 'header': {'title': ''}}, 'row': {'type': 'nominal', 'field': 'column'}, 'x': {'type': 'quantitative', 'field': 'probability'}, 'y': {'type': 'nominal', 'axis': {'title': '𝛾 value'}, 'field': 'value'}}, 'resolve': {'scale': {'y': 'independent'}}, 'title': 'Probability distributions of comparison vector values', '$schema': 'https://vega.github.io/schema/vega-lite/v3.4.0.json'}

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

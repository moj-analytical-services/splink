# Want to account for the possibility that some settings may
# have m probabilities but no u probabilities

# Need to also account for nulls within m and u probabilities

# Need to issue warning if probabilities given are exactly equal to starting values
import statistics
from copy import deepcopy

from .settings import ComparisonColumn, Settings
from .params import Params

from splink.charts import load_chart_definition, altair_if_installed_else_json


class CombineEstimates:
    def __init__(self, params_list: list, estimate_names: list):

        self.settings_list = [p.params for p in params_list]
        self.named_settings_dict = dict(zip(estimate_names, self.settings_list))

    def groups_of_comparison_columns_by_name(self):
        """
        The user inputs a list of parameter estimates, each of which
        contains a settings dict

        If the input data is:
        Params list element 1:  Estimate name 'forename blocking'
            Comparison columns: [surname, dob, email]
        Params list element 2:  Estimate name 'surname blocking'
            Comparison columns: [forename, dob, email]
        Params list element 3:  Estimate name 'dob blocking'
            Comparison columns: [forename, surname, email]

        We want to group by comparison column name, respecting the fact
        that not all params have all comparison columns

        This function gives you back a dict in the form:
        {
            "forename": {"surname blocking": cc, "dob_blocking": cc},
            "surname": [etc]
            "dob": [etc]
            "email": [etc]

        }
        """
        combined_cc = {}
        # For each model which has been estimated
        for estimate_name, settings_estimate in self.named_settings_dict.items():
            # For each comparison column in this model
            for cc_name, cc in settings_estimate.comparison_column_dict.items():
                # Create or add to dict which contains the different estimate
                # for this column, using estimate names as keys
                if cc_name not in combined_cc:
                    combined_cc[cc_name] = {}
                combined_cc[cc_name][estimate_name] = cc
        return combined_cc

    def _zip_m_and_u_probabilities(self, cc_estimates: list):
        """Groups together the different estimates of the same parameter.

        e.g. turns:
        [{"m_probabilities":[ma0,ma1],{"m_probabilities":[ua0,ua1],...},
         {"m_probabilities":[mb0,mb1],{"m_probabilities":[ub0,ub1],...}]

        into

        {"zipped_m": [(ma0,mb0), (ma1, mb1)],
         "zipped_u": [(ua0,ub0), (ua1, ub1)]}
        """

        zipped_m_probs = zip(*[cc["m_probabilities"] for cc in cc_estimates])
        zipped_u_probs = zip(*[cc["u_probabilities"] for cc in cc_estimates])
        return {"zipped_m": zipped_m_probs, "zipped_u": zipped_u_probs}

    def combine_estimates_single_cc(self, cc_estimates: list, reduce_function=None):
        """cc_estimates is a list of the different estaimtes for a single comparison column
        e.g. all of the different comparison columns for forename from params_list
        """

        if reduce_function is None:
            reduce_function = statistics.median

        zipped = self._zip_m_and_u_probabilities(cc_estimates)

        m_probs = [reduce_function(estimates) for estimates in zipped["zipped_m"]]
        u_probs = [reduce_function(estimates) for estimates in zipped["zipped_u"]]

        cc = deepcopy(cc_estimates[0].column_dict)
        cc["m_probabilities"] = m_probs
        cc["u_probabilities"] = u_probs
        return ComparisonColumn(cc)

    def get_combined_settings(self, reduce_function=None):

        new_settings = deepcopy(self.settings_list[0].settings_dict)

        new_comparison_columns = []
        gathered = self.groups_of_comparison_columns_by_name()

        # For each comparison column (first name, surname etc)
        for dict_of_ccs in gathered.values():
            ccs = list(dict_of_ccs.values())
            # Take the average of each parameter estimates
            combined = self.combine_estimates_single_cc(ccs, reduce_function)
            new_comparison_columns.append(combined.column_dict)

        new_settings["comparison_columns"] = new_comparison_columns

        new_blocking_rules = []
        for settings_dict in self.settings_list:
            new_blocking_rules.extend(settings_dict["blocking_rules"])

        new_settings["blocking_rules"] = new_blocking_rules
        return new_settings

    def summary_report(self, reduce_function=None, summary_name="combined"):

        lines = []
        gathered = self.groups_of_comparison_columns_by_name()

        combined_settings = self.get_combined_settings(reduce_function=reduce_function)
        combined_settings = Settings(combined_settings)

        for cc_name, dict_of_ccs in gathered.items():

            lines.append(f"Column name: {cc_name}")

            lines.append(f"    m probabilities")
            for estimate_name, cc in dict_of_ccs.items():
                m_probs = cc["m_probabilities"]
                m_probs = [f"{p:.4g}" for p in m_probs]
                lines.append(f"        {estimate_name:<15}: {m_probs}")

            cc = combined_settings.get_comparison_column(cc_name)
            m_probs = cc["m_probabilities"]
            m_probs = [f"{p:.4g}" for p in m_probs]
            summary = f"{summary_name} value:"
            lines.append(f"        {summary:<15}: {m_probs}")

            lines.append(f"    u probabilities")

            for estimate_name, cc in dict_of_ccs.items():
                m_probs = cc["u_probabilities"]
                m_probs = [f"{p:.4g}" for p in m_probs]
                lines.append(f"        {estimate_name:<15}: {m_probs}")

            cc = combined_settings.get_comparison_column(cc_name)
            m_probs = cc["u_probabilities"]
            m_probs = [f"{p:.4g}" for p in m_probs]
            summary = f"{summary_name} value:"
            lines.append(f"        {summary:<15}: {m_probs}")

        return "\n".join(lines)

    def estimates_as_rows(self):
        """A list of dicts represeting
        all the param estimates which can be passed
        t"""
        rows = []

        gathered = self.groups_of_comparison_columns_by_name()
        for cc_name, dict_of_ccs in gathered.items():
            for estimate_name, cc in dict_of_ccs.items():
                new_rows = cc.as_rows()
                for r in new_rows:
                    r["estimate_name"] = estimate_name
                rows.extend(new_rows)

        return rows

    def comparison_chart(self):
        chart_def = load_chart_definition("compare_estimates.json")
        chart_def["data"]["values"] = self.estimates_as_rows()

        return altair_if_installed_else_json(chart_def)

    def __repr__(self):
        return self.summary_report(summary_name="median")

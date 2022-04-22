from copy import deepcopy
import logging

from .expectation_maximisation import expectation_maximisation
from .misc import bayes_factor_to_prob, prob_to_bayes_factor
from .parse_sql import get_columns_used_from_sql
from .blocking import block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .charts import (
    m_u_values_interactive_history_chart,
    match_weights_interactive_history_chart,
    proportion_of_matches_iteration_chart,
)

logger = logging.getLogger(__name__)


class EMTrainingSession:
    def __init__(
        self,
        linker,
        blocking_rule_for_training,
        fix_u_probabilities=False,
        fix_m_probabilities=False,
        fix_proportion_of_matches=False,
        comparisons_to_deactivate=None,
        comparison_levels_to_reverse_blocking_rule=None,
    ):

        self.original_settings_obj = linker.settings_obj
        self.original_linker = linker
        self.training_linker = deepcopy(linker)

        self.settings_obj = self.training_linker.settings_obj
        self.settings_obj._retain_matching_columns = False
        self.settings_obj._retain_intermediate_calculation_columns = False
        self.settings_obj._training_mode = True

        self.settings_obj._blocking_rule_for_training = blocking_rule_for_training
        self.blocking_rule_for_training = blocking_rule_for_training

        if comparison_levels_to_reverse_blocking_rule:
            self.comparison_levels_to_reverse_blocking_rule = (
                comparison_levels_to_reverse_blocking_rule
            )
        else:
            self.comparison_levels_to_reverse_blocking_rule = self.original_settings_obj._get_comparison_levels_corresponding_to_training_blocking_rule(  # noqa
                blocking_rule_for_training
            )

        self.settings_obj._proportion_of_matches = (
            self._blocking_adjusted_proportion_of_matches
        )

        self._training_fix_u_probabilities = fix_u_probabilities
        self._training_fix_m_probabilities = fix_m_probabilities
        self._training_fix_proportion_of_matches = fix_proportion_of_matches

        # Remove comparison columns which are either 'used up' by the blocking rules
        # or alternatively, if the user has manually provided a list to remove,
        # use this instead
        if not comparisons_to_deactivate:
            comparisons_to_deactivate = []
            br_cols = get_columns_used_from_sql(blocking_rule_for_training)
            for cc in self.settings_obj.comparisons:
                cc_cols = cc.input_columns_used_by_case_statement
                cc_cols = [c.input_name for c in cc_cols]
                if set(br_cols).intersection(cc_cols):
                    comparisons_to_deactivate.append(cc)
        cc_names_to_deactivate = [
            cc.comparison_name for cc in comparisons_to_deactivate
        ]
        self.comparisons_that_cannot_be_estimated = comparisons_to_deactivate

        filtered_ccs = [
            cc
            for cc in self.settings_obj.comparisons
            if cc.comparison_name not in cc_names_to_deactivate
        ]

        self.settings_obj.comparisons = filtered_ccs
        self.comparisons_that_can_be_estimated = filtered_ccs

        self.comparison_level_history = []
        self.lambda_history = []
        self.add_iteration()

    def _training_log_message(self):
        not_estimated = [
            cc.comparison_name for cc in self.comparisons_that_cannot_be_estimated
        ]
        not_estimated = "".join([f"\n    - {cc}" for cc in not_estimated])

        estimated = [
            cc.comparison_name for cc in self.comparisons_that_can_be_estimated
        ]
        estimated = "".join([f"\n    - {cc}" for cc in estimated])

        if self._training_fix_m_probabilities and self._training_fix_u_probabilities:
            raise ValueError("Can't train model if you fix both m and u probabilites")
        elif self._training_fix_u_probabilities:
            mu = "m probabilities"
        elif self._training_fix_m_probabilities:
            mu = "u probabilities"
        else:
            mu = "m and u probabilities"

        logger.info(
            "\n----- Starting EM training session -----\n\n"
            f"Training the {mu} of the model by blocking on:\n"
            f"{self.blocking_rule_for_training}\n\n"
            "Parameter estimates will be made for the following comparison(s):"
            f"{estimated}\n"
            "\nParameter estimates cannot be made for the following comparison(s)"
            f" since they are used in the blocking rules: {not_estimated}"
        )

    def _comparison_vectors(self):
        self._training_log_message()

        sql = block_using_rules_sql(self.training_linker)
        self.training_linker.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self.settings_obj)
        self.training_linker.enqueue_sql(sql, "__splink__df_comparison_vectors")
        return self.training_linker.execute_sql_pipeline([])

    def train(self):

        cvv = self._comparison_vectors()

        # Compute the new params, populating the paramters in the copied settings object
        # At this stage, we do not overwrite any of the parameters
        # in the original (main) setting object
        expectation_maximisation(self, cvv)

        training_desc = f"EM, blocked on: {self.blocking_rule_for_training}"

        # Add m and u values to original settings
        for cc in self.settings_obj.comparisons:
            orig_cc = self.original_settings_obj._get_comparison_by_name(
                cc.comparison_name
            )
            for cl in cc.comparison_levels_excluding_null:

                orig_cl = orig_cc.get_comparison_level_by_comparison_vector_value(
                    cl.comparison_vector_value
                )
                # TODO:  HERE'S WHERE THE LOGGING SHOULD HAPPEN
                if not self._training_fix_m_probabilities:
                    not_observed = "level not observed in training dataset"
                    if cl._m_probability == not_observed:
                        orig_cl.add_trained_m_probability(not_observed, training_desc)
                        logger.info(
                            f"m probability not trained for {cc.comparison_name} - "
                            f"{cl.label_for_charts} (comparison vector value: "
                            f"{cl.comparison_vector_value}). This usually means the "
                            "comparison level was never observed in the training data."
                        )
                    else:
                        orig_cl.add_trained_m_probability(
                            cl.m_probability, training_desc
                        )

                if not self._training_fix_u_probabilities:
                    not_observed = "level not observed in training dataset"
                    if cl._u_probability == not_observed:
                        orig_cl.add_trained_u_probability(not_observed, training_desc)
                        logger.info(
                            f"u probability not trained for {cc.comparison_name} - "
                            f"{cl.label_for_charts} (comparison vector value: "
                            f"{cl.comparison_vector_value}). This usually means the "
                            "comparison level was never observed in the training data."
                        )
                    else:
                        orig_cl.add_trained_u_probability(
                            cl.u_probability, training_desc
                        )

        self.original_linker.em_training_sessions.append(self)

    def add_iteration(self):

        ccs = [deepcopy(cc) for cc in self.settings_obj.comparisons]
        self.comparison_level_history.append(ccs)

        lam = self.settings_obj._proportion_of_matches

        record = {
            "proportion_of_matches": lam,
            "proportion_of_matches_reciprocal": 1 / lam,
        }

        self.lambda_history.append(record)

    @property
    def _blocking_adjusted_proportion_of_matches(self):

        adj_bayes_factor = prob_to_bayes_factor(
            self.original_settings_obj._proportion_of_matches
        )

        comp_levels = self.comparison_levels_to_reverse_blocking_rule
        if not comp_levels:
            comp_levels = self.original_settings_obj._get_comparison_levels_corresponding_to_training_blocking_rule(  # noqa
                self.blocking_rule_for_training
            )

        for cl in comp_levels:
            adj_bayes_factor = cl.bayes_factor * adj_bayes_factor

        return bayes_factor_to_prob(adj_bayes_factor)

    @property
    def iteration_history_records(self):
        output_records = []

        for iteration, ccs in enumerate(self.comparison_level_history):
            for cc in ccs:
                records = cc.as_detailed_records

                for r in records:
                    r["iteration"] = iteration
                    r[
                        "proportion_of_matches"
                    ] = self.settings_obj._proportion_of_matches
                output_records.extend(records)
        return output_records

    @property
    def lambda_history_records(self):
        output_records = []
        for i, r in enumerate(self.lambda_history):
            r = deepcopy(r)
            r["iteration"] = i
            output_records.append(r)
        return output_records

    def proportion_of_matches_iteration_chart(self):
        records = self.lambda_history_records
        return proportion_of_matches_iteration_chart(records)

    def match_weights_interactive_history_chart(self):
        records = self.iteration_history_records
        return match_weights_interactive_history_chart(records)

    def m_u_values_interactive_history_chart(self):
        records = self.iteration_history_records
        return m_u_values_interactive_history_chart(records)

    def max_change_message(self, max_change_dict):
        message = "Largest change in params was"

        if max_change_dict["max_change_type"] == "proportion_of_matches":
            message = (
                f"{message} {max_change_dict['max_change_value']:,.3g} in "
                "proportion_of_matches"
            )
        else:
            cl = max_change_dict["current_comparison_level"]
            m_u = max_change_dict["max_change_type"]
            cc_name = cl.comparison.comparison_name
            cl_label = cl.label_for_charts
            level_text = f"{cc_name}, level `{cl_label}`"

            message = (
                f"{message} {max_change_dict['max_change_value']:,.3g} in "
                f"the {m_u} of {level_text}"
            )

        return message

    def max_change_in_parameters_comparison_levels(self):

        previous_iteration = self.comparison_level_history[-2]
        this_iteration = self.comparison_level_history[-1]
        max_change = -0.1

        max_change_levels = {
            "previous_iteration": None,
            "this_iteration": None,
            "max_change_type": None,
            "max_change_value": None,
        }
        comparisons = zip(previous_iteration, this_iteration)
        for comparison in comparisons:
            prev_cc = comparison[0]
            this_cc = comparison[1]
            z_cls = zip(prev_cc.comparison_levels, this_cc.comparison_levels)
            for z_cl in z_cls:
                if z_cl[0].is_null_level:
                    continue
                prev_cl = z_cl[0]
                this_cl = z_cl[1]
                change_m = this_cl.m_probability - prev_cl.m_probability
                change_u = this_cl.u_probability - prev_cl.u_probability
                change = max(abs(change_m), abs(change_u))
                change_type = (
                    "m_probability"
                    if abs(change_m) > abs(change_u)
                    else "u_probability"
                )
                change_value = change_m if abs(change_m) > abs(change_u) else change_u
                if change > max_change:
                    max_change = change
                    max_change_levels["prev_comparison_level"] = prev_cl
                    max_change_levels["current_comparison_level"] = this_cl
                    max_change_levels["max_change_type"] = change_type
                    max_change_levels["max_change_value"] = change_value
                    max_change_levels["max_abs_change_value"] = abs(change_value)

        previous_iteration = self.lambda_history[-2]["proportion_of_matches"]
        this_iteration = self.lambda_history[-1]["proportion_of_matches"]
        change_proportion_of_matches = this_iteration - previous_iteration

        if abs(change_proportion_of_matches) > max_change:
            max_change = abs(change_proportion_of_matches)
            max_change_levels["prev_comparison_level"] = None
            max_change_levels["current_comparison_level"] = None
            max_change_levels["max_change_type"] = "proportion_of_matches"
            max_change_levels["max_change_value"] = change_proportion_of_matches
            max_change_levels["max_abs_change_value"] = abs(
                change_proportion_of_matches
            )

        max_change_levels["message"] = self.max_change_message(max_change_levels)

        return max_change_levels

    def __repr__(self):
        deactivated_cols = ", ".join(
            [cc.comparison_name for cc in self.comparisons_that_cannot_be_estimated]
        )
        return (
            f"<EMTrainingSession, blocking on {self.blocking_rule_for_training}, "
            f"deactivating comparisons {deactivated_cols}>"
        )

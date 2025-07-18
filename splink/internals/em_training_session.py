from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from splink.internals.blocking import BlockingRule, block_using_rules_sqls
from splink.internals.charts import (
    ChartReturnType,
    m_u_parameters_interactive_history_chart,
    match_weights_interactive_history_chart,
    probability_two_random_records_match_iteration_chart,
)
from splink.internals.comparison import Comparison
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.constants import LEVEL_NOT_OBSERVED_TEXT
from splink.internals.input_column import InputColumn
from splink.internals.misc import bayes_factor_to_prob, prob_to_bayes_factor
from splink.internals.parse_sql import get_columns_used_from_sql
from splink.internals.pipeline import CTEPipeline
from splink.internals.settings import (
    ComparisonAndLevelDict,
    CoreModelSettings,
    Settings,
    TrainingSettings,
)
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from .database_api import DatabaseAPISubClass
from .exceptions import EMTrainingException
from .expectation_maximisation import expectation_maximisation

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker
    from splink.internals.splink_dataframe import SplinkDataFrame


class EMTrainingSession:
    """Manages training models using the Expectation Maximisation algorithm, and
    holds statistics on the evolution of parameter estimates.  Plots diagnostic charts
    """

    def __init__(
        self,
        # TODO: remove linker arg once we unpick the two places we use it
        linker: Linker,
        db_api: DatabaseAPISubClass,
        blocking_rule_for_training: BlockingRule,
        core_model_settings: CoreModelSettings,
        training_settings: TrainingSettings,
        unique_id_input_columns: List[InputColumn],
        fix_u_probabilities: bool = False,
        fix_m_probabilities: bool = False,
        fix_probability_two_random_records_match: bool = False,
        estimate_without_term_frequencies: bool = False,
    ):
        logger.info("\n----- Starting EM training session -----\n")

        self._original_linker = linker
        self.db_api = db_api

        self.training_settings = training_settings
        self.unique_id_input_columns = unique_id_input_columns

        self.original_core_model_settings = core_model_settings.copy()

        if not isinstance(blocking_rule_for_training, BlockingRule):
            blocking_rule_for_training = BlockingRule(
                blocking_rule_for_training, linker._sql_dialect_str
            )

        self._blocking_rule_for_training = blocking_rule_for_training
        self.estimate_without_term_frequencies = estimate_without_term_frequencies

        self._comparison_levels_to_reverse_blocking_rule: list[
            ComparisonAndLevelDict
        ] = Settings._get_comparison_levels_corresponding_to_training_blocking_rule(  # noqa
            blocking_rule_sql=blocking_rule_for_training.blocking_rule_sql,
            sqlglot_dialect=self.db_api.sql_dialect.sqlglot_dialect,
            comparisons=core_model_settings.comparisons,
        )

        # batch together fixed probabilities rather than keep hold of the bools
        self.training_fixed_probabilities: set[str] = {
            probability_type
            for (probability_type, fixed) in [
                ("m", fix_m_probabilities),
                ("u", fix_u_probabilities),
                ("lambda", fix_probability_two_random_records_match),
            ]
            if fixed
        }

        # Remove comparison columns which are either 'used up' by the blocking rules
        comparisons_to_deactivate = []
        br_cols = get_columns_used_from_sql(
            blocking_rule_for_training.blocking_rule_sql,
            self.db_api.sql_dialect.sqlglot_dialect,
        )
        for cc in core_model_settings.comparisons:
            cc_cols = cc._input_columns_used_by_case_statement
            cc_cols = [c.input_name for c in cc_cols]
            if set(br_cols).intersection(cc_cols):
                comparisons_to_deactivate.append(cc)
        cc_names_to_deactivate = [
            cc.output_column_name for cc in comparisons_to_deactivate
        ]
        self._comparisons_that_cannot_be_estimated: list[Comparison] = (
            comparisons_to_deactivate
        )

        filtered_ccs = [
            cc
            for cc in core_model_settings.comparisons
            if cc.output_column_name not in cc_names_to_deactivate
        ]

        core_model_settings.comparisons = filtered_ccs
        core_model_settings.probability_two_random_records_match = (
            self._blocking_adjusted_probability_two_random_records_match
        )

        # this should be fixed:
        self.columns_to_select_for_comparison_vector_values = (
            Settings.columns_to_select_for_comparison_vector_values(
                unique_id_input_columns=self.unique_id_input_columns,
                comparisons=core_model_settings.comparisons,
                retain_matching_columns=False,
                additional_columns_to_retain=[],
                needs_matchkey_column=False,
            )
        )

        self.core_model_settings = core_model_settings
        # initial params get inserted in training
        self._core_model_settings_history: List[CoreModelSettings] = []

    def _training_log_message(self):
        not_estimated = [
            cc.output_column_name for cc in self._comparisons_that_cannot_be_estimated
        ]
        not_estimated_str = "".join([f"\n    - {cc}" for cc in not_estimated])

        estimated = [
            cc.output_column_name for cc in self.core_model_settings.comparisons
        ]
        estimated_str = "".join([f"\n    - {cc}" for cc in estimated])

        if {"m", "u"}.issubset(self.training_fixed_probabilities):
            raise ValueError("Can't train model if you fix both m and u probabilites")
        elif "u" in self.training_fixed_probabilities:
            mu = "m probabilities"
        elif "m" in self.training_fixed_probabilities:
            mu = "u probabilities"
        else:
            mu = "m and u probabilities"

        blocking_rule = self._blocking_rule_for_training.blocking_rule_sql

        logger.info(
            f"Estimating the {mu} of the model by blocking on:\n"
            f"{blocking_rule}\n\n"
            "Parameter estimates will be made for the following comparison(s):"
            f"{estimated_str}\n"
            "\nParameter estimates cannot be made for the following comparison(s)"
            f" since they are used in the blocking rules: {not_estimated_str}"
        )

    def _comparison_vectors(self) -> SplinkDataFrame:
        self._training_log_message()

        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self._original_linker, pipeline)
        pipeline = CTEPipeline([nodes_with_tf])

        orig_settings = self._original_linker._settings_obj
        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            blocking_rules=[self._blocking_rule_for_training],
            link_type=orig_settings._link_type,
            source_dataset_input_column=orig_settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=orig_settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocked_pairs = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([blocked_pairs, nodes_with_tf])

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            orig_settings._columns_to_select_for_blocking,
            self.columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=orig_settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=orig_settings.column_info_settings.unique_id_input_column,
        )

        pipeline.enqueue_list_of_sqls(sqls)
        return self.db_api.sql_pipeline_to_splink_dataframe(pipeline)

    def _train(self, cvv: SplinkDataFrame = None) -> CoreModelSettings:
        if cvv is None:
            cvv = self._comparison_vectors()

        # check that the blocking rule actually generates _some_ record pairs,
        # if not give the user a helpful message
        if not cvv.as_record_dict(limit=1):
            br_sql = f"`{self._blocking_rule_for_training.blocking_rule_sql}`"
            raise EMTrainingException(
                f"Training rule {br_sql} resulted in no record pairs.  "
                "This means that in the supplied data set "
                f"there were no pairs of records for which {br_sql} was `true`.\n"
                "Expectation maximisation requires a substantial number of record "
                "comparisons to produce accurate parameter estimates - usually "
                "at least a few hundred, but preferably at least a few thousand.\n"
                "You must revise your training blocking rule so that the set of "
                "generated comparisons is not empty.  You can use "
                "`count_comparisons_from_blocking_rule()` to compute "
                "the number of comparisons that will be generated by a blocking rule."
            )

        # Compute the new params, populating the paramters in the copied settings object
        # At this stage, we do not overwrite any of the parameters
        # in the original (main) setting object
        core_model_settings_history = expectation_maximisation(
            db_api=self.db_api,
            training_settings=self.training_settings,
            estimate_without_term_frequencies=self.estimate_without_term_frequencies,
            core_model_settings=self.core_model_settings,
            unique_id_input_columns=self.unique_id_input_columns,
            training_fixed_probabilities=self.training_fixed_probabilities,
            df_comparison_vector_values=cvv,
        )
        self.core_model_settings = core_model_settings_history[-1]
        self._core_model_settings_history = core_model_settings_history

        rule = self._blocking_rule_for_training.blocking_rule_sql
        training_desc = f"EM, blocked on: {rule}"

        # we have a copy of the original core model settings - this is what we
        # add trained values too, and return.
        original_core_model_settings = self.original_core_model_settings
        # Add m and u values to original settings
        for cc in self.core_model_settings.comparisons:
            orig_cc = original_core_model_settings.get_comparison_by_output_column_name(
                cc.output_column_name
            )
            for cl in cc._comparison_levels_excluding_null:
                orig_cl = orig_cc._get_comparison_level_by_comparison_vector_value(
                    cl.comparison_vector_value
                )

                if "m" not in self.training_fixed_probabilities:
                    not_observed = LEVEL_NOT_OBSERVED_TEXT
                    if cl._m_probability == not_observed:
                        orig_cl._add_trained_m_probability(not_observed, training_desc)
                        logger.info(
                            f"m probability not trained for {cc.output_column_name} - "
                            f"{cl.label_for_charts} (comparison vector value: "
                            f"{cl.comparison_vector_value}). This usually means the "
                            "comparison level was never observed in the training data."
                        )
                    else:
                        orig_cl._add_trained_m_probability(
                            cl.m_probability, training_desc
                        )

                if "u" not in self.training_fixed_probabilities:
                    not_observed = LEVEL_NOT_OBSERVED_TEXT
                    if cl._u_probability == not_observed:
                        orig_cl._add_trained_u_probability(not_observed, training_desc)
                        logger.info(
                            f"u probability not trained for {cc.output_column_name} - "
                            f"{cl.label_for_charts} (comparison vector value: "
                            f"{cl.comparison_vector_value}). This usually means the "
                            "comparison level was never observed in the training data."
                        )
                    else:
                        orig_cl._add_trained_u_probability(
                            cl.u_probability, training_desc
                        )
        return original_core_model_settings

    @property
    def _blocking_adjusted_probability_two_random_records_match(self):
        orig_prop_m = (
            self.original_core_model_settings.probability_two_random_records_match
        )

        adj_bayes_factor = prob_to_bayes_factor(orig_prop_m)

        logger.log(15, f"Original prob two random records match: {orig_prop_m:.3f}")

        comp_level_infos = self._comparison_levels_to_reverse_blocking_rule

        for comp_level_info in comp_level_infos:
            cl = comp_level_info["level"]
            comparison = comp_level_info["comparison"]
            adj_bayes_factor = cl._bayes_factor * adj_bayes_factor

            logger.log(
                15,
                f"Increasing prob two random records match using "
                f"{comparison.output_column_name} - {cl.label_for_charts}"
                f" using bayes factor {cl._bayes_factor:,.3f}",
            )

        adjusted_prop_m = bayes_factor_to_prob(adj_bayes_factor)
        logger.log(
            15,
            f"\nProb two random records match adjusted for blocking on "
            f"{self._blocking_rule_for_training.blocking_rule_sql}: "
            f"{adjusted_prop_m:.3f}",
        )
        return adjusted_prop_m

    @property
    def _iteration_history_records(self):
        output_records = []

        for iteration, core_model_settings in enumerate(
            self._core_model_settings_history
        ):
            records = core_model_settings.parameters_as_detailed_records

            for r in records:
                r["iteration"] = iteration
                # TODO: why lambda from current settings, not history?
                r["probability_two_random_records_match"] = (
                    self.core_model_settings.probability_two_random_records_match
                )

            output_records.extend(records)
        return output_records

    @property
    def _lambda_history_records(self):
        output_records = []
        for i, s in enumerate(self._core_model_settings_history):
            lam = s.probability_two_random_records_match
            r = {
                "probability_two_random_records_match": lam,
                "probability_two_random_records_match_reciprocal": 1 / lam,
                "iteration": i,
            }

            output_records.append(r)
        return output_records

    def probability_two_random_records_match_iteration_chart(self) -> ChartReturnType:
        """
        Display a chart showing the iteration history of the probability that two
        random records match.

        Returns:
            An interactive Altair chart.
        """
        records = self._lambda_history_records
        return probability_two_random_records_match_iteration_chart(records)

    def match_weights_interactive_history_chart(self) -> ChartReturnType:
        """
        Display an interactive chart of the match weights history.

        Returns:
            An interactive Altair chart.
        """
        records = self._iteration_history_records
        return match_weights_interactive_history_chart(
            records, blocking_rule=self._blocking_rule_for_training.blocking_rule_sql
        )

    def m_u_values_interactive_history_chart(self) -> ChartReturnType:
        """
        Display an interactive chart of the m and u values.

        Returns:
            An interactive Altair chart.
        """
        records = self._iteration_history_records
        return m_u_parameters_interactive_history_chart(records)

    def __repr__(self):
        deactivated_cols = ", ".join(
            [cc.output_column_name for cc in self._comparisons_that_cannot_be_estimated]
        )
        blocking_rule = self._blocking_rule_for_training.blocking_rule_sql
        return (
            f"<EMTrainingSession, blocking on {blocking_rule}, "
            f"deactivating comparisons {deactivated_cols}>"
        )

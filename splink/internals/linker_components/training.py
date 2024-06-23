from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Union

from splink.internals.blocking import (
    BlockingRule,
    SaltedBlockingRule,
)
from splink.internals.blocking_analysis import (
    _cumulative_comparisons_to_be_scored_from_blocking_rules,
)
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.blocking_rule_creator_utils import to_blocking_rule_creator
from splink.internals.comparison import Comparison
from splink.internals.comparison_level import ComparisonLevel
from splink.internals.em_training_session import EMTrainingSession
from splink.internals.estimate_u import estimate_u_values
from splink.internals.m_from_labels import estimate_m_from_pairwise_labels
from splink.internals.m_training import estimate_m_values_from_label_column
from splink.internals.misc import (
    ensure_is_iterable,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import (
    compute_df_concat_with_tf,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


class LinkerTraining:
    """Estimate the parameters of the linkage model, accessed via
    `linker.training`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def estimate_probability_two_random_records_match(
        self,
        deterministic_matching_rules: List[Union[str, BlockingRuleCreator]],
        recall: float,
        max_rows_limit: int = int(1e9),
    ) -> None:
        """Estimate the model parameter `probability_two_random_records_match` using
        a direct estimation approach.

        See [here](https://github.com/moj-analytical-services/splink/issues/462)
        for discussion of methodology

        Args:
            deterministic_matching_rules (list): A list of deterministic matching
                rules that should be designed to admit very few (none if possible)
                false positives
            recall (float): A guess at the recall the deterministic matching rules
                will attain.  i.e. what proportion of true matches will be recovered
                by these deterministic rules
        """

        if (recall > 1) or (recall <= 0):
            raise ValueError(
                f"Estimated recall must be greater than 0 "
                f"and no more than 1. Supplied value {recall}."
            ) from None

        deterministic_matching_rules = ensure_is_iterable(deterministic_matching_rules)
        blocking_rules: List[BlockingRule] = []
        for br in deterministic_matching_rules:
            blocking_rules.append(
                to_blocking_rule_creator(br).get_blocking_rule(
                    self._linker._db_api.sql_dialect.name
                )
            )

        pd_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
            splink_df_dict=self._linker._input_tables_dict,
            blocking_rules=blocking_rules,
            link_type=self._linker._settings_obj._link_type,
            db_api=self._linker._db_api,
            max_rows_limit=max_rows_limit,
            unique_id_input_column=self._linker._settings_obj.column_info_settings.unique_id_input_column,
            source_dataset_input_column=self._linker._settings_obj.column_info_settings.source_dataset_input_column,
        )

        records = pd_df.to_dict(orient="records")

        summary_record = records[-1]
        num_observed_matches = summary_record["cumulative_rows"]
        num_total_comparisons = summary_record["cartesian"]

        if num_observed_matches > num_total_comparisons * recall:
            raise ValueError(
                f"Deterministic matching rules led to more "
                f"observed matches than is consistent with supplied recall. "
                f"With these rules, recall must be at least "
                f"{num_observed_matches/num_total_comparisons:,.2f}."
            )

        num_expected_matches = num_observed_matches / recall
        prob = num_expected_matches / num_total_comparisons

        # warn about boundary values, as these will usually be in error
        if num_observed_matches == 0:
            logger.warning(
                f"WARNING: Deterministic matching rules led to no observed matches! "
                f"This means that no possible record pairs are matches, "
                f"and no records are linked to one another.\n"
                f"If this is truly the case then you do not need "
                f"to run the linkage model.\n"
                f"However this is usually in error; "
                f"expected rules to have recall of {100*recall:,.0f}%. "
                f"Consider revising rules as they may have an error."
            )
        if prob == 1:
            logger.warning(
                "WARNING: Probability two random records match is estimated to be 1.\n"
                "This means that all possible record pairs are matches, "
                "and all records are linked to one another.\n"
                "If this is truly the case then you do not need "
                "to run the linkage model.\n"
                "However, it is more likely that this estimate is faulty. "
                "Perhaps your deterministic matching rules include "
                "too many false positives?"
            )

        self._linker._settings_obj._probability_two_random_records_match = prob

        reciprocal_prob = "Infinity" if prob == 0 else f"{1/prob:,.2f}"
        logger.info(
            f"Probability two random records match is estimated to be  {prob:.3g}.\n"
            f"This means that amongst all possible pairwise record comparisons, one in "
            f"{reciprocal_prob} are expected to match.  "
            f"With {num_total_comparisons:,.0f} total"
            " possible comparisons, we expect a total of around "
            f"{num_expected_matches:,.2f} matching pairs"
        )

    def estimate_u_using_random_sampling(
        self, max_pairs: float = 1e6, seed: int = None
    ) -> None:
        """Estimate the u parameters of the linkage model using random sampling.

        The u parameters represent the proportion of record comparisons that fall
        into each comparison level amongst truly non-matching records.

        This procedure takes a sample of the data and generates the cartesian
        product of pairwise record comparisons amongst the sampled records.
        The validity of the u values rests on the assumption that the resultant
        pairwise comparisons are non-matches (or at least, they are very unlikely to be
        matches). For large datasets, this is typically true.

        The results of estimate_u_using_random_sampling, and therefore an entire splink
        model, can be made reproducible by setting the seed parameter. Setting the seed
        will have performance implications as additional processing is required.

        Args:
            max_pairs (int): The maximum number of pairwise record comparisons to
                sample. Larger will give more accurate estimates but lead to longer
                runtimes.  In our experience at least 1e9 (one billion) gives best
                results but can take a long time to compute. 1e7 (ten million)
                is often adequate whilst testing different model specifications, before
                the final model is estimated.
            seed (int): Seed for random sampling. Assign to get reproducible u
                probabilities. Note, seed for random sampling is only supported for
                DuckDB and Spark, for Athena and SQLite set to None.

        Examples:
            ```py
            linker.estimate_u_using_random_sampling(1e8)
            ```

        Returns:
            None: Updates the estimated u parameters within the linker object
            and returns nothing.
        """
        if max_pairs == 1e6:
            # keep default value small so as not to take too long, but warn users
            logger.warning(
                "You are using the default value for `max_pairs`, "
                "which may be too small and thus lead to inaccurate estimates for your "
                "model's u-parameters. Consider increasing to 1e8 or 1e9, which will "
                "result in more accurate estimates, but with a longer run time."
            )

        estimate_u_values(self._linker, max_pairs, seed)
        self._linker._populate_m_u_from_trained_values()

        self._linker._settings_obj._columns_without_estimated_parameters_message()

    def estimate_parameters_using_expectation_maximisation(
        self,
        blocking_rule: Union[str, BlockingRuleCreator],
        comparisons_to_deactivate: list[Comparison] = None,
        comparison_levels_to_reverse_blocking_rule: list[ComparisonLevel] = None,
        estimate_without_term_frequencies: bool = False,
        fix_probability_two_random_records_match: bool = False,
        fix_m_probabilities: bool = False,
        fix_u_probabilities: bool = True,
        populate_probability_two_random_records_match_from_trained_values: bool = False,
    ) -> EMTrainingSession:
        """Estimate the parameters of the linkage model using expectation maximisation.

        By default, the m probabilities are estimated, but not the u probabilities,
        because good estimates for the u probabilities can be obtained from
        `linker.estimate_u_using_random_sampling()`.  You can change this by setting
        `fix_u_probabilities` to False.

        The blocking rule provided is used to generate pairwise record comparisons.
        Usually, this should be a blocking rule that results in a dataframe where
        matches are between about 1% and 99% of the comparisons.

        By default, m parameters are estimated for all comparisons except those which
        are included in the blocking rule.

        For example, if the blocking rule is `l.first_name = r.first_name`, then
        parameter esimates will be made for all comparison except those which use
        `first_name` in their sql_condition

        By default, the probability two random records match is estimated for the
        blocked data, and then the m and u parameters for the columns specified in the
        blocking rules are used to estiamte the global probability two random records
        match.

        To control which comparisons should have their parameter estimated, and the
        process of 'reversing out' the global probability two random records match, the
        user may specify `comparisons_to_deactivate` and
        `comparison_levels_to_reverse_blocking_rule`.   This is useful, for example
        if you block on the dmetaphone of a column but match on the original column.

        Examples:
            Default behaviour
            ```py
            br_training = "l.first_name = r.first_name and l.dob = r.dob"
            linker.training.estimate_parameters_using_expectation_maximisation(br_training)
            ```
            Specify which comparisons to deactivate
            ```py
            br_training = "l.dmeta_first_name = r.dmeta_first_name"
            settings_obj = linker._settings_obj
            comp = settings_obj._get_comparison_by_output_column_name("first_name")
            dmeta_level = comp._get_comparison_level_by_comparison_vector_value(1)
            linker.training.estimate_parameters_using_expectation_maximisation(
                br_training,
                comparisons_to_deactivate=["first_name"],
                comparison_levels_to_reverse_blocking_rule=[dmeta_level],
            )
            ```

        Args:
            blocking_rule (BlockingRuleCreator | str): The blocking rule used to
                generate pairwise record comparisons.
            comparisons_to_deactivate (list, optional): By default, splink will
                analyse the blocking rule provided and estimate the m parameters for
                all comaprisons except those included in the blocking rule.  If
                comparisons_to_deactivate are provided, spink will instead
                estimate m parameters for all comparison except those specified
                in the comparisons_to_deactivate list.  This list can either contain
                the output_column_name of the Comparison as a string, or Comparison
                objects.  Defaults to None.
            comparison_levels_to_reverse_blocking_rule (list, optional): By default,
                splink will analyse the blocking rule provided and adjust the
                global probability two random records match to account for the matches
                specified in the blocking rule. If provided, this argument will overrule
                this default behaviour. The user must provide a list of ComparisonLevel
                objects.  Defaults to None.
            estimate_without_term_frequencies (bool, optional): If True, the iterations
                of the EM algorithm ignore any term frequency adjustments and only
                depend on the comparison vectors. This allows the EM algorithm to run
                much faster, but the estimation of the parameters will change slightly.
            fix_probability_two_random_records_match (bool, optional): If True, do not
                update the probability two random records match after each iteration.
                Defaults to False.
            fix_m_probabilities (bool, optional): If True, do not update the m
                probabilities after each iteration. Defaults to False.
            fix_u_probabilities (bool, optional): If True, do not update the u
                probabilities after each iteration. Defaults to True.
            populate_probability_two_random_records_match_from_trained_values (bool,optional): If
                True, derive this parameter from the blocked value. Defaults to False.

        Examples:
            ```py
            blocking_rule = "l.first_name = r.first_name and l.dob = r.dob"
            linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)
            ```
            or using pre-built rules
            ```py
            from splink.duckdb.blocking_rule_library import block_on
            blocking_rule = block_on(["first_name", "surname"])
            linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)
            ```

        Returns:
            EMTrainingSession:  An object containing information about the training
                session such as how parameters changed during the iteration history

        """  # noqa: E501
        # Ensure this has been run on the main linker so that it's in the cache
        # to be used by the training linkers
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(self._linker, pipeline)

        blocking_rule_obj = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
            self._linker._sql_dialect
        )

        if type(blocking_rule_obj) not in (BlockingRule, SaltedBlockingRule):
            # TODO: seems a mismatch between message and type re: SaltedBlockingRule
            raise TypeError(
                "EM blocking rules must be plain blocking rules, not "
                "salted or exploding blocking rules"
            )

        if comparisons_to_deactivate:
            # If user provided a string, convert to Comparison object
            comparisons_to_deactivate = [
                (
                    self._linker._settings_obj._get_comparison_by_output_column_name(n)
                    if isinstance(n, str)
                    else n
                )
                for n in comparisons_to_deactivate
            ]
            if comparison_levels_to_reverse_blocking_rule is None:
                logger.warning(
                    "\nWARNING: \n"
                    "You have provided comparisons_to_deactivate but not "
                    "comparison_levels_to_reverse_blocking_rule.\n"
                    "If comparisons_to_deactivate is provided, then "
                    "you usually need to provide corresponding "
                    "comparison_levels_to_reverse_blocking_rule "
                    "because each comparison to deactivate is effectively treated "
                    "as an exact match."
                )

        em_training_session = EMTrainingSession(
            self._linker,
            db_api=self._linker._db_api,
            blocking_rule_for_training=blocking_rule_obj,
            core_model_settings=self._linker._settings_obj.core_model_settings,
            training_settings=self._linker._settings_obj.training_settings,
            unique_id_input_columns=self._linker._settings_obj.column_info_settings.unique_id_input_columns,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            fix_probability_two_random_records_match=fix_probability_two_random_records_match,  # noqa 501
            comparisons_to_deactivate=comparisons_to_deactivate,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,  # noqa 501
            estimate_without_term_frequencies=estimate_without_term_frequencies,
        )

        core_model_settings = em_training_session._train()
        # overwrite with the newly trained values in our linker settings
        self._linker._settings_obj.core_model_settings = core_model_settings
        self._linker._em_training_sessions.append(em_training_session)

        self._linker._populate_m_u_from_trained_values()

        if populate_probability_two_random_records_match_from_trained_values:
            self._linker._populate_probability_two_random_records_match_from_trained_values()

        self._linker._settings_obj._columns_without_estimated_parameters_message()

        return em_training_session

    def estimate_m_from_pairwise_labels(self, labels_splinkdataframe_or_table_name):
        """Estimate the m parameters of the linkage model from a dataframe of pairwise
        labels.

        The table of labels should be in the following format, and should
        be registered with your database:
        |source_dataset_l|unique_id_l|source_dataset_r|unique_id_r|
        |----------------|-----------|----------------|-----------|
        |df_1            |1          |df_2            |2          |
        |df_1            |1          |df_2            |3          |

        Note that `source_dataset` and `unique_id` should correspond to the
        values specified in the settings dict, and the `input_table_aliases`
        passed to the `linker` object. Note that at the moment, this method does
        not respect values in a `clerical_match_score` column.  If provided, these
        are ignored and it is assumed that every row in the table of labels is a score
        of 1, i.e. a perfect match.

        Args:
          labels_splinkdataframe_or_table_name (str): Name of table containing labels
            in the database or SplinkDataframe

        Examples:
            ```py
            pairwise_labels = pd.read_csv("./data/pairwise_labels_to_estimate_m.csv")
            linker.table_management.register_table(
                pairwise_labels, "labels", overwrite=True
            )
            linker.estimate_m_from_pairwise_labels("labels")
            ```
        """
        labels_tablename = self._linker._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        estimate_m_from_pairwise_labels(self._linker, labels_tablename)

    def estimate_m_from_label_column(self, label_colname: str) -> None:
        """Estimate the m parameters of the linkage model from a label (ground truth)
        column in the input dataframe(s).

        The m parameters represent the proportion of record comparisons that fall
        into each comparison level amongst truly matching records.

        The ground truth column is used to generate pairwise record comparisons
        which are then assumed to be matches.

        For example, if the entity being matched is persons, and your input dataset(s)
        contain social security number, this could be used to estimate the m values
        for the model.

        Note that this column does not need to be fully populated.  A common case is
        where a unique identifier such as social security number is only partially
        populated.

        Args:
            label_colname (str): The name of the column containing the ground truth
                label in the input data.

        Examples:
            ```py
            linker.training.estimate_m_from_label_column("social_security_number")
            ```

        Returns:
            None: Updates the estimated m parameters within the linker object.
        """

        # Ensure this has been run on the main linker so that it can be used by
        # training linker when it checks the cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(self._linker, pipeline)

        estimate_m_values_from_label_column(
            self._linker,
            label_colname,
        )
        self._linker._populate_m_u_from_trained_values()

        self._linker._settings_obj._columns_without_estimated_parameters_message()

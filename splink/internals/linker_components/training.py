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

        This method counts the number of matches found using deterministic rules and
        divides by the total number of possible record comparisons. The recall of the
        deterministic rules is used to adjust this proportion up to reflect missed
        matches, providing an estimate of the probability that two random records from
        the input data are a match.

        Note that if more than one deterministic rule is provided, any duplicate
        pairs are automatically removed, so you do not need to worry about double
        counting.

        See [here](https://github.com/moj-analytical-services/splink/issues/462)
        for discussion of methodology.

        Args:
            deterministic_matching_rules (list): A list of deterministic matching
                rules designed to admit very few (preferably no) false positives.
            recall (float): An estimate of the recall the deterministic matching
                rules will achieve, i.e., the proportion of all true matches these
                rules will recover.
            max_rows_limit (int): Maximum number of rows to consider during estimation.
                Defaults to 1e9.

        Examples:
            ```py
            deterministic_rules = [
                block_on("forename", "dob"),
                "l.forename = r.forename and levenshtein(r.surname, l.surname) <= 2",
                block_on("email")
            ]
            linker.training.estimate_probability_two_random_records_match(
                deterministic_rules, recall=0.8
            )
            ```
        Returns:
            Nothing: Updates the estimated parameter within the linker object and
                returns nothing.
        """

        if (recall > 1) or (recall <= 0):
            raise ValueError(
                f"Estimated recall must be greater than 0 "
                f"and no more than 1. Supplied value {recall}."
            ) from None

        deterministic_matching_rules_list: list[str | BlockingRuleCreator] = (
            ensure_is_iterable(deterministic_matching_rules)
        )
        blocking_rules: List[BlockingRule] = []
        for br in deterministic_matching_rules_list:
            blocking_rules.append(
                to_blocking_rule_creator(br).get_blocking_rule(
                    self._linker._db_api.sql_dialect.sql_dialect_str
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

        The u parameters estimate the proportion of record comparisons that fall
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
            linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
            ```

        Returns:
            Nothing: Updates the estimated u parameters within the linker object and
                returns nothing.
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
        estimate_without_term_frequencies: bool = False,
        fix_probability_two_random_records_match: bool = False,
        fix_m_probabilities: bool = False,
        fix_u_probabilities: bool = True,
        populate_probability_two_random_records_match_from_trained_values: bool = False,
    ) -> EMTrainingSession:
        """Estimate the parameters of the linkage model using expectation maximisation.

        By default, the m probabilities are estimated, but not the u probabilities,
        because good estimates for the u probabilities can be obtained from
        `linker.training.estimate_u_using_random_sampling()`.  You can change this by
        setting `fix_u_probabilities` to False.

        The blocking rule provided is used to generate pairwise record comparisons.
        Usually, this should be a blocking rule that results in a dataframe where
        matches are between about 1% and 99% of the blocked comparisons.

        By default, m parameters are estimated for all comparisons except those which
        are included in the blocking rule.

        For example, if the blocking rule is `block_on("first_name")`, then
        parameter estimates will be made for all comparison except those which use
        `first_name` in their sql_condition

        By default, the probability two random records match is allowed to vary
        during EM estimation, but is not saved back to the model.  See
        [this PR](https://github.com/moj-analytical-services/splink/pull/734) for
        the rationale.



        Args:
            blocking_rule (BlockingRuleCreator | str): The blocking rule used to
                generate pairwise record comparisons.
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
            populate_probability_two_random_records_match_from_trained_values (bool, optional):
                If True, derive this parameter from the blocked value. Defaults to False.

        Examples:
            ```py
            br_training = block_on("first_name", "dob")
            linker.training.estimate_parameters_using_expectation_maximisation(
                br_training
            )
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
            self._linker._sql_dialect_str
        )

        if not isinstance(blocking_rule_obj, (BlockingRule, SaltedBlockingRule)):
            raise TypeError(
                "EM blocking rules must be plain blocking rules, not "
                "exploding blocking rules"
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
            fix_probability_two_random_records_match=fix_probability_two_random_records_match,
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
        """Estimate the m probabilities of the linkage model from a dataframe of
        pairwise labels.

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

            linker.training.estimate_m_from_pairwise_labels("labels")
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
            Nothing: Updates the estimated m parameters within the linker object.
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

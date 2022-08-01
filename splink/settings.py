import logging
from copy import deepcopy
from typing import List
from .parse_sql import get_columns_used_from_sql
from .misc import prob_to_bayes_factor, prob_to_match_weight, dedupe_preserving_order
from .charts import m_u_parameters_chart, match_weights_chart
from .comparison import Comparison
from .comparison_level import ComparisonLevel
from .default_from_jsonschema import default_value_from_schema
from .input_column import InputColumn
from .validate_jsonschema import validate_settings_against_schema
from .blocking import BlockingRule

logger = logging.getLogger(__name__)


class Settings:
    """The settings object contains the configuration and parameters of the data
    linking model"""

    def __init__(self, settings_dict):

        settings_dict = deepcopy(settings_dict)

        # If incoming comparisons are of type Comparison not dict, turn back into dict
        ccs = settings_dict["comparisons"]
        ccs = [cc.as_dict() if isinstance(cc, Comparison) else cc for cc in ccs]

        settings_dict["comparisons"] = ccs

        # In incoming comparisons have nested ComparisonLevels, turn back into dict
        for comparison_dict in settings_dict["comparisons"]:
            comparison_dict["comparison_levels"] = [
                cl.as_dict() if isinstance(cl, ComparisonLevel) else cl
                for cl in comparison_dict["comparison_levels"]
            ]

        validate_settings_against_schema(settings_dict)

        self._settings_dict = settings_dict

        ccs = self._settings_dict["comparisons"]
        s_else_d = self._from_settings_dict_else_default
        self._sql_dialect = s_else_d("sql_dialect")

        self.comparisons: List[Comparison] = []
        for cc in ccs:
            self.comparisons.append(Comparison(cc, self))

        self._link_type = s_else_d("link_type")
        self._probability_two_random_records_match = s_else_d(
            "probability_two_random_records_match"
        )
        self._em_convergence = s_else_d("em_convergence")
        self._max_iterations = s_else_d("max_iterations")
        self._unique_id_column_name = s_else_d("unique_id_column_name")

        self._retain_matching_columns = s_else_d("retain_matching_columns")
        self._retain_intermediate_calculation_columns = s_else_d(
            "retain_intermediate_calculation_columns"
        )

        brs_as_strings = s_else_d("blocking_rules_to_generate_predictions")

        self._blocking_rules_to_generate_predictions = self._brs_as_objs(brs_as_strings)

        self._gamma_prefix = s_else_d("comparison_vector_value_column_prefix")
        self._bf_prefix = s_else_d("bayes_factor_column_prefix")
        self._tf_prefix = s_else_d("term_frequency_adjustment_column_prefix")
        self._blocking_rule_for_training = None
        self._training_mode = False

        self._warn_if_no_null_level_in_comparisons()

    def __deepcopy__(self, memo) -> "Settings":
        """When we do EM training, we need a copy of the Settings which is independent
        of the original e.g. modifying the copy will not affect the original.
        This method implements ensures the Settings can be deepcopied."""
        cc = Settings(self.as_dict())
        return cc

    def _from_settings_dict_else_default(self, key):
        # Don't want a default of None because that's a valid value sometimes
        # i.e. need to distinguish between None and 'not found in settings dict'
        val = self._settings_dict.get(key, "__val_not_found_in_settings_dict__")
        if val == "__val_not_found_in_settings_dict__":
            val = default_value_from_schema(key, "root")
        return val

    def _warn_if_no_null_level_in_comparisons(self):
        for c in self.comparisons:
            if not c._has_null_level:
                logger.warning(
                    "Warning: No null level found for comparison "
                    f"{c._output_column_name}.\n"
                    "In most cases you want to define a comparison level that deals"
                    " with the case that one or both sides of the comparison are null."
                    "\nThis comparison level should have the `is_null_level` flag to "
                    "True in the settings for that comparison level"
                    "\nIf the column does not contain null values, or you know what "
                    "you're doing, you can ignore this warning"
                )

    @property
    def _additional_columns_to_retain(self):
        cols = self._from_settings_dict_else_default("additional_columns_to_retain")
        return [InputColumn(c, tf_adjustments=False, settings_obj=self) for c in cols]

    @property
    def _source_dataset_column_name_is_required(self):
        return self._link_type not in [
            "dedupe_only",
            "link_only_find_matches_to_new_records",
        ]

    @property
    def _source_dataset_column_name(self):
        if self._source_dataset_column_name_is_required:
            s_else_d = self._from_settings_dict_else_default
            return s_else_d("source_dataset_column_name")
        else:
            return None

    @property
    def _unique_id_input_columns(self) -> List[InputColumn]:
        cols = []

        if self._source_dataset_column_name_is_required:
            col = InputColumn(
                self._source_dataset_column_name,
                settings_obj=self,
            )
            cols.append(col)

        col = InputColumn(self._unique_id_column_name, settings_obj=self)
        cols.append(col)

        return cols

    @property
    def _term_frequency_columns(self) -> List[InputColumn]:
        cols = set()
        for cc in self.comparisons:
            cols.update(cc._tf_adjustment_input_col_names)
        return [
            InputColumn(c, settings_obj=self, tf_adjustments=True) for c in list(cols)
        ]

    @property
    def _needs_matchkey_column(self) -> bool:
        """Where multiple `blocking_rules_to_generate_predictions` are specified,
        it's useful to include a matchkey column, that indicates from which blocking
        rule the pairwise record comparisons arose.

        This column is only needed if multiple rules are specified.
        """

        return len(self._blocking_rules_to_generate_predictions) > 1

    @property
    def _columns_used_by_comparisons(self):
        cols_used = []
        if self._source_dataset_column_name_is_required:
            cols_used.append(self._source_dataset_column_name)
        cols_used.append(self._unique_id_column_name)
        for cc in self.comparisons:
            cols = cc._input_columns_used_by_case_statement
            cols = [c.name() for c in cols]

            cols_used.extend(cols)
        return dedupe_preserving_order(cols_used)

    @property
    def _columns_to_select_for_blocking(self):
        cols = []

        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.l_name_as_l())
        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.r_name_as_r())

        for cc in self.comparisons:
            cols.extend(cc._columns_to_select_for_blocking)

        for add_col in self._additional_columns_to_retain:
            cols.extend(add_col.l_r_names_as_l_r())

        return dedupe_preserving_order(cols)

    @property
    def _columns_to_select_for_comparison_vector_values(self):
        cols = []

        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_l())
        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_r())

        for cc in self.comparisons:
            cols.extend(cc._columns_to_select_for_comparison_vector_values)

        for add_col in self._additional_columns_to_retain:
            cols.extend(add_col.names_l_r())

        if self._needs_matchkey_column:
            cols.append("match_key")

        cols = dedupe_preserving_order(cols)
        return cols

    @property
    def _columns_to_select_for_bayes_factor_parts(self):
        cols = []

        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_l())
        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_r())

        for cc in self.comparisons:
            cols.extend(cc._columns_to_select_for_bayes_factor_parts)

        for add_col in self._additional_columns_to_retain:
            cols.extend(add_col.names_l_r())

        if self._needs_matchkey_column:
            cols.append("match_key")

        cols = dedupe_preserving_order(cols)
        return cols

    @property
    def _columns_to_select_for_predict(self):
        cols = []

        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_l())
        for uid_col in self._unique_id_input_columns:
            cols.append(uid_col.name_r())

        for cc in self.comparisons:
            cols.extend(cc._columns_to_select_for_predict)

        for add_col in self._additional_columns_to_retain:
            cols.extend(add_col.names_l_r())

        if self._needs_matchkey_column:
            cols.append("match_key")

        cols = dedupe_preserving_order(cols)
        return cols

    def _get_comparison_by_output_column_name(self, name):
        for cc in self.comparisons:
            if cc._output_column_name == name:
                return cc
        raise ValueError(f"No comparison column with name {name}")

    def _brs_as_objs(self, brs_as_strings):
        brs_as_objs = []
        for br in brs_as_strings:
            if isinstance(br, dict):
                br = BlockingRule(
                    br["blocking_rule"], salting_partitions=br["salting_partitions"]
                )
                br.preceding_rules = brs_as_objs.copy()
                brs_as_objs.append(br)
            else:
                br = BlockingRule(br)
                br.preceding_rules = brs_as_objs.copy()
                brs_as_objs.append(br)

        return brs_as_objs

    def _get_comparison_levels_corresponding_to_training_blocking_rule(
        self, blocking_rule
    ):
        """
        If we block on (say) first name and surname, then all blocked comparisons are
        guaranteed to have a match on first name and surname

        The probability two random records match must be adjusted for the fact this is a
        subset of the comparisons

        To correctly adjust, we need to find one or more comparison levels corresponding
        to the blocking rule and use their bayes factor

        In the example, we need to find a comparison level for an exact match on first
        name, and one for an exact match on surname

        Or alternatively (and preferably, to avoid correlation issues), a comparison
        level for an exact match on first_name AND surname.   i.e. a single level for
        exact match on full name

        """
        blocking_exact_match_columns = set(get_columns_used_from_sql(blocking_rule))

        ccs = self.comparisons

        exact_comparison_levels = []
        for cc in ccs:
            for cl in cc.comparison_levels:
                if cl._is_exact_match:
                    exact_comparison_levels.append(cl)

        # Where exact match on multiple columns exists, use that instead of individual
        # exact match columns
        # So for example, if we have a param estimate for exact match on first name AND
        # surname, prefer that
        # over individual estimtes for exact match first name and surname.
        exact_comparison_levels.sort(key=lambda x: -len(x._exact_match_colnames))

        comparison_levels_corresponding_to_blocking_rule = []
        for cl in exact_comparison_levels:
            exact_cols = set(cl._exact_match_colnames)
            if exact_cols.issubset(blocking_exact_match_columns):
                blocking_exact_match_columns = blocking_exact_match_columns - exact_cols
                comparison_levels_corresponding_to_blocking_rule.append(cl)

        return comparison_levels_corresponding_to_blocking_rule

    @property
    def _parameters_as_detailed_records(self):
        output = []
        for i, cc in enumerate(self.comparisons):
            records = cc._as_detailed_records
            for r in records:
                r[
                    "probability_two_random_records_match"
                ] = self._probability_two_random_records_match
                r["comparison_sort_order"] = i
            output.extend(records)

        prior_description = (
            "The probability that two random records drawn at random match is "
            f"{self._probability_two_random_records_match:.3f} or one in "
            f" {1/self._probability_two_random_records_match:,.1f} records."
            "This is equivalent to a starting match weight of "
            f"{prob_to_match_weight(self._probability_two_random_records_match):.3f}."
        )

        # Finally add a record for probability_two_random_records_match
        rr_match = self._probability_two_random_records_match
        prop_record = {
            "comparison_name": "probability_two_random_records_match",
            "sql_condition": None,
            "label_for_charts": "",
            "m_probability": None,
            "u_probability": None,
            "m_probability_description": None,
            "u_probability_description": None,
            "has_tf_adjustments": False,
            "tf_adjustment_column": None,
            "tf_adjustment_weight": None,
            "is_null_level": False,
            "bayes_factor": prob_to_bayes_factor(
                self._probability_two_random_records_match
            ),
            "log2_bayes_factor": prob_to_match_weight(
                self._probability_two_random_records_match
            ),
            "comparison_vector_value": 0,
            "max_comparison_vector_value": 0,
            "bayes_factor_description": prior_description,
            "probability_two_random_records_match": rr_match,
            "comparison_sort_order": -1,
        }
        output.insert(0, prop_record)
        return output

    @property
    def _parameter_estimates_as_records(self):
        output = []
        for i, cc in enumerate(self.comparisons):
            records = cc._parameter_estimates_as_records
            for r in records:
                r["comparison_sort_order"] = i
            output.extend(records)
        return output

    def as_dict(self):
        """Serialise the current settings (including any estimated model parameters)
        to a dictionary, enabling the settings to be saved to disk and reloaded
        """
        rr_match = self._probability_two_random_records_match
        current_settings = {
            "comparisons": [cc.as_dict() for cc in self.comparisons],
            "probability_two_random_records_match": rr_match,
        }
        return {**self._settings_dict, **current_settings}

    def _as_completed_dict(self):
        rr_match = self._probability_two_random_records_match
        current_settings = {
            "comparisons": [cc._as_completed_dict() for cc in self.comparisons],
            "probability_two_random_records_match": rr_match,
            "unique_id_column_name": self._unique_id_column_name,
            "source_dataset_column_name": self._source_dataset_column_name,
        }
        return {**self._settings_dict, **current_settings}

    def match_weights_chart(self, as_dict=False):
        records = self._parameters_as_detailed_records

        return match_weights_chart(records, as_dict=as_dict)

    def m_u_parameters_chart(self, as_dict=False):
        records = self._parameters_as_detailed_records
        return m_u_parameters_chart(records, as_dict=as_dict)

    def _columns_without_estimated_parameters_message(self):
        message_lines = []
        for c in self.comparisons:
            msg = c._is_trained_message
            if msg is not None:
                message_lines.append(c._is_trained_message)

        if len(message_lines) == 0:
            message = (
                "\nYour model is fully trained. All comparisons have at least "
                "one estimate for their m and u values"
            )
        else:
            message = "\nYour model is not yet fully trained. Missing estimates for:"
            message_lines.insert(0, message)
            message = "\n".join(message_lines)

        logger.info(message)

    @property
    def _is_fully_trained(self):
        return all([c._is_trained for c in self.comparisons])

    def _not_trained_messages(self):
        messages = []
        for c in self.comparisons:
            messages.extend(c._not_trained_messages)
        return messages

    @property
    def human_readable_description(self):
        comparison_descs = [
            c._human_readable_description_succinct for c in self.comparisons
        ]
        comparison_descs = "\n".join(comparison_descs)
        desc = (
            "SUMMARY OF LINKING MODEL\n"
            "------------------------\n"
            "The similarity of pairwise record comparison in your model will be "
            f"assessed as follows:\n\n{comparison_descs}"
        )
        return desc

    @property
    def salting_required(self):
        for br in self._blocking_rules_to_generate_predictions:
            if br.salting_partitions > 1:
                return True
        return False

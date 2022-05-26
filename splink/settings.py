import logging
from copy import deepcopy
from .parse_sql import get_columns_used_from_sql
from .misc import prob_to_bayes_factor, prob_to_match_weight
from .charts import m_u_parameters_chart, match_weights_chart
from .comparison import Comparison
from .default_from_jsonschema import default_value_from_schema
from .input_column import InputColumn
from .misc import dedupe_preserving_order
from .validate_jsonschema import validate_settings_against_schema

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

        validate_settings_against_schema(settings_dict)

        self._settings_dict = settings_dict

        ccs = self._settings_dict["comparisons"]
        s_else_d = self._from_settings_dict_else_default
        self._sql_dialect = s_else_d("sql_dialect")

        self.comparisons: list[Comparison] = []
        for cc in ccs:
            self.comparisons.append(Comparison(cc, self))

        self._link_type = s_else_d("link_type")
        self._proportion_of_matches = s_else_d("proportion_of_matches")
        self._em_convergence = s_else_d("em_convergence")
        self._max_iterations = s_else_d("max_iterations")
        self._unique_id_column_name = s_else_d("unique_id_column_name")

        self._retain_matching_columns = s_else_d("retain_matching_columns")
        self._retain_intermediate_calculation_columns = s_else_d(
            "retain_intermediate_calculation_columns"
        )
        self._blocking_rules_to_generate_predictions = s_else_d(
            "blocking_rules_to_generate_predictions"
        )
        self._gamma_prefix = s_else_d("comparison_vector_value_column_prefix")
        self._bf_prefix = s_else_d("bayes_factor_column_prefix")
        self._tf_prefix = s_else_d("term_frequency_adjustment_column_prefix")
        self._blocking_rule_for_training = None
        self._training_mode = False

    def __deepcopy__(self, memo):
        cc = Settings(self.as_dict())
        return cc

    def _from_settings_dict_else_default(self, key):
        val = self._settings_dict.get(key, "__val_not_found_in_settings_dict__")
        if val == "__val_not_found_in_settings_dict__":
            val = default_value_from_schema(key, "root")
        return val

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
    def _unique_id_input_columns(self):
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
    def _term_frequency_columns(self):
        cols = set()
        for cc in self.comparisons:
            cols.update(cc._tf_adjustment_input_col_names)
        return list(cols)

    @property
    def _needs_matchkey_column(self):

        return len(self._blocking_rules_to_generate_predictions) > 1

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

        return cols

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

    def _get_comparison_levels_corresponding_to_training_blocking_rule(
        self, blocking_rule
    ):
        """
        If we block on (say) first name and surname, then all blocked comparisons are
        guaranteed to have a match on first name and surname

        The proportion of matches must be adjusted for the fact this is a subset of the
        comparisons

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
                r["proportion_of_matches"] = self._proportion_of_matches
                r["comparison_sort_order"] = i
            output.extend(records)

        prior_description = (
            f"Proportion of matches is {self._proportion_of_matches:.3f}, equivalent "
            f"to a starting match weight of "
            f"{prob_to_match_weight(self._proportion_of_matches):.3f}."
            "This means that if two records are drawn at random, one in "
            f" {1/self._proportion_of_matches:,.1f} is expected to be a match"
        )

        # Finally add a record for proportion of matches
        prop_record = {
            "comparison_name": "proportion_of_matches",
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
            "bayes_factor": prob_to_bayes_factor(self._proportion_of_matches),
            "log2_bayes_factor": prob_to_match_weight(self._proportion_of_matches),
            "comparison_vector_value": 0,
            "max_comparison_vector_value": 0,
            "bayes_factor_description": prior_description,
            "proportion_of_matches": self._proportion_of_matches,
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
        current_settings = {
            "comparisons": [cc.as_dict() for cc in self.comparisons],
            "proportion_of_matches": self._proportion_of_matches,
        }
        return {**self._settings_dict, **current_settings}

    def as_completed_dict(self):
        current_settings = {
            "comparisons": [cc.as_completed_dict() for cc in self.comparisons],
            "proportion_of_matches": self._proportion_of_matches,
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

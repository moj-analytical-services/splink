from typing import TYPE_CHECKING

from .comparison_level import ComparisonLevel
from .misc import dedupe_preserving_order

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .settings import Settings


class Comparison:
    def __init__(self, comparison_dict, settings_obj: "Settings" = None):

        # Protected because we don't want to modify
        self._comparison_dict = comparison_dict
        comparison_level_list = comparison_dict["comparison_levels"]
        self.comparison_levels = [
            ComparisonLevel(i, self, settings_obj) for i in comparison_level_list
        ]
        self.settings_obj = settings_obj

        # Assign comparison vector values starting at highest level, count down to 0
        num_levels = len([cl for cl in self.comparison_levels if not cl.is_null_level])
        self.num_levels = num_levels
        counter = num_levels - 1

        for level in self.comparison_levels:
            if level.is_null_level:
                level.comparison_vector_value = -1
                level.max_level = False
            else:
                level.comparison_vector_value = counter
                if counter == num_levels - 1:
                    level.max_level = True
                else:
                    level.max_level = False
                counter -= 1

    def __deepcopy__(self, memo):
        cc = Comparison(self.as_dict, self.settings_obj)
        return cc

    @property
    def comparison_levels_excluding_null(self):
        return [cl for cl in self.comparison_levels if not cl.is_null_level]

    @property
    def gamma_prefix(self):
        return self.settings_obj._gamma_prefix

    @property
    def retain_intermediate_calculation_columns(self):
        return self.settings_obj._retain_intermediate_calculation_columns

    @property
    def bf_column_name(self):
        return f"{self.settings_obj._bf_prefix}{self.comparison_name}"

    @property
    def bf_tf_adj_column_name(self):
        bf = self.settings_obj._bf_prefix
        tf = self.settings_obj._tf_prefix
        cc_name = self.comparison_name
        return f"{bf}{tf}adj_{cc_name}"

    @property
    def has_tf_adjustments(self):
        return any([cl.has_tf_adjustments for cl in self.comparison_levels])

    @property
    def case_statement(self):

        sqls = [
            cl.when_then_comparison_vector_value_sql for cl in self.comparison_levels
        ]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END as {self.gamma_column_name}"

        return sql

    @property
    def input_columns_used_by_case_statement(self):
        cols = []
        for cl in self.comparison_levels:
            cols.extend(cl.input_columns_used_by_sql_condition)

        # dedupe_preserving_order on input column
        already_observed = []
        deduped_cols = []
        for col in cols:
            if col.input_name not in already_observed:
                deduped_cols.append(col)
                already_observed.append(col.input_name)

        return deduped_cols

    @property
    def comparison_name(self):
        if "column_name" in self._comparison_dict:
            return self._comparison_dict["column_name"]
        else:
            cols = self.input_columns_used_by_case_statement
            cols = [c.input_name for c in cols]
            if len(cols) == 1:
                return cols[0]
            else:
                return f"custom_{'_'.join(cols)}"

    @property
    def gamma_column_name(self):
        return f"{self.gamma_prefix}{self.comparison_name}"

    @property
    def tf_adjustment_input_col_names(self):
        cols = [cl.tf_adjustment_input_column_name for cl in self.comparison_levels]
        cols = [c for c in cols if c]

        return cols

    @property
    def columns_to_select_for_blocking(self):
        cols = []
        for cl in self.comparison_levels:
            cols.extend(cl.columns_to_select_for_blocking)

        return dedupe_preserving_order(cols)

    @property
    def columns_to_select_for_comparison_vector_values(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl.input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self.settings_obj._retain_matching_columns:
                output_cols.extend(col.names_l_r)

        output_cols.append(self.case_statement)

        for col in input_cols:

            if self.has_tf_adjustments:
                output_cols.extend(col.tf_name_l_r)

        return dedupe_preserving_order(output_cols)

    @property
    def columns_to_select_for_bayes_factor_parts(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl.input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self.settings_obj._retain_matching_columns:

                output_cols.extend(col.names_l_r)

        output_cols.append(self.gamma_column_name)

        for col in input_cols:

            if self.has_tf_adjustments:
                if self.settings_obj._retain_intermediate_calculation_columns:

                    output_cols.extend(col.tf_name_l_r)

        # Bayes factor case when statement
        sqls = [cl.bayes_factor_sql for cl in self.comparison_levels]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END as {self.bf_column_name} "
        output_cols.append(sql)

        # tf adjustment case when statement

        if self.has_tf_adjustments:
            sqls = [cl.tf_adjustment_sql for cl in self.comparison_levels]
            sql = " ".join(sqls)
            sql = f"CASE {sql} END as {self.bf_tf_adj_column_name} "
            output_cols.append(sql)
        output_cols.append(self.gamma_column_name)

        return dedupe_preserving_order(output_cols)

    @property
    def columns_to_select_for_predict(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl.input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self.settings_obj._retain_matching_columns:
                output_cols.extend(col.names_l_r)

        if (
            self.settings_obj._training_mode
            or self.settings_obj._retain_matching_columns
        ):
            output_cols.append(self.gamma_column_name)

        for col in input_cols:
            if self.settings_obj._retain_intermediate_calculation_columns:
                if self.has_tf_adjustments:

                    output_cols.extend(col.tf_name_l_r)

                output_cols.extend(self.match_weight_columns_to_multiply)

        return dedupe_preserving_order(output_cols)

    @property
    def match_weight_columns_to_multiply(self):
        cols = []
        cols.append(self.bf_column_name)
        if self.has_tf_adjustments:
            cols.append(self.bf_tf_adj_column_name)
        return cols

    @property
    def term_frequency_columns(self):
        cols = set()
        for cl in self.comparison_levels:
            cols.add(cl.tf_adjustment_input_col_name)
        return list(cols)

    @property
    def as_dict(self):
        return {
            "column_name": self.comparison_name,
            "comparison_levels": [cl.as_dict for cl in self.comparison_levels],
        }

    @property
    def as_completed_dict(self):
        return {
            "column_name": self.comparison_name,
            "comparison_levels": [
                cl.as_completed_dict for cl in self.comparison_levels
            ],
            "input_columns_used_by_case_statement": [
                c.input_name for c in self.input_columns_used_by_case_statement
            ],
        }

    @property
    def has_estimated_m_values(self):
        return all(cl.has_estimated_m_values for cl in self.comparison_levels)

    @property
    def has_estimated_u_values(self):
        return all(cl.has_estimated_u_values for cl in self.comparison_levels)

    @property
    def all_m_are_trained(self):
        return all(cl.m_is_trained for cl in self.comparison_levels)

    @property
    def all_u_are_trained(self):
        return all(cl.u_is_trained for cl in self.comparison_levels)

    @property
    def some_m_are_trained(self):
        return any(cl.m_is_trained for cl in self.comparison_levels_excluding_null)

    @property
    def some_u_are_trained(self):
        return any(cl.u_is_trained for cl in self.comparison_levels_excluding_null)

    @property
    def is_trained_message(self):
        messages = []
        if self.all_m_are_trained and self.all_u_are_trained:
            return None

        if not self.some_u_are_trained:
            messages.append("no u values are trained")
        elif self.some_u_are_trained and not self.all_u_are_trained:
            messages.append("some u values are not trained")

        if not self.some_m_are_trained:
            messages.append("no m values are trained")
        elif self.some_m_are_trained and not self.all_m_are_trained:
            messages.append("some m values are not trained")

        message = ", ".join(messages)
        message = f"    - {self.comparison_name} ({message})."
        return message

    @property
    def is_trained(self):
        return self.all_m_are_trained and self.all_u_are_trained

    @property
    def as_detailed_records(self):
        records = []
        for cl in self.comparison_levels:
            record = {}
            record["comparison_name"] = self.comparison_name
            record = {**record, **cl.as_detailed_record}
            records.append(record)
        return records

    @property
    def parameter_estimates_as_records(self):
        records = []
        for cl in self.comparison_levels:
            new_records = cl.parameter_estimates_as_records
            for r in new_records:
                r["comparison_name"] = self.comparison_name
            records.extend(new_records)

        return records

    def get_comparison_level_by_comparison_vector_value(self, value):
        for cl in self.comparison_levels:

            if cl.comparison_vector_value == value:
                return cl
        raise ValueError(f"No comparison level with comparison vector value {value}")

    def __repr__(self):
        return (
            f"<Comparison {self.comparison_name} with "
            f"{self.num_levels} levels at {hex(id(self))}>"
        )

    @property
    def not_trained_messages(self):

        msgs = []

        cname = self.comparison_name

        header = f"Comparison: '{cname}':\n"

        msg_template = "{header}    {m_or_u} values not fully trained"

        if not self.all_m_are_trained:
            msgs.append(msg_template.format(header=header, m_or_u="m"))
        if not self.all_u_are_trained:
            msgs.append(msg_template.format(header=header, m_or_u="u"))

        return msgs

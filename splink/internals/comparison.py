from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, List, Optional

from splink.internals.misc import (
    dedupe_preserving_order,
    join_list_with_commas_final_and,
)

from .comparison_level import ComparisonLevel, _default_m_values, _default_u_values

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.settings import ColumnInfoSettings


class Comparison:
    """Each Comparison defines how data from one or more input columns is
    compared to assess its similarity.

    For example, one Comparison may represent how similarity is assessed for a
    person's date of birth.  Others may represent the comparison of a person's name or
    location.

    The method used to assess similarity will depend on the type of data -
    for instance, the method used to assess similarity of a company's turnover would
    be different to the method used to assess the similarity of a person's first name.

    A linking model thus usually contains several Comparisons.

    As far as possible, Comparisons should be configured to satisfy the assumption of
    independece conditional on the true match status, a key assumption of the Fellegi
    Sunter probabilistic linkage model.  This would be broken, for example, if a model
    contained one Comparison for city, and another for postcode. Instead, in this
    example, a single comparison should be modelled, which may to capture similarity
    taking account of both the city and postcode field.

    Each Comparison contains two or more `ComparisonLevel`s which define the gradations
    of similarity between the input columns within the Comparison.

    For example, for the date of birth Comparison there may be a ComparisonLevel for an
    exact match, another for a one-character difference, and another for all other
    comparisons.

    To summarise:

    ```
    Data Linking Model
    ├─-- Comparison: Date of birth
    │    ├─-- ComparisonLevel: Exact match
    │    ├─-- ComparisonLevel: One character difference
    │    ├─-- ComparisonLevel: All other
    ├─-- Comparison: Name
    │    ├─-- ComparisonLevel: Exact match on first name and surname
    │    ├─-- ComparisonLevel: Exact match on first name
    │    ├─-- etc.
    ```

    """

    def __init__(
        self,
        comparison_levels: List[ComparisonLevel | dict[str, Any]],
        sqlglot_dialect: str,
        output_column_name: str = None,
        comparison_description: str = None,
        column_info_settings: ColumnInfoSettings = None,
    ):
        comparison_levels_as_objs: list[ComparisonLevel] = [
            ComparisonLevel(**cl, sqlglot_dialect=sqlglot_dialect)
            if isinstance(cl, dict)
            else cl
            for cl in comparison_levels
        ]
        self.comparison_levels: list[ComparisonLevel] = comparison_levels_as_objs

        self._column_info_settings: Optional[ColumnInfoSettings] = column_info_settings

        self.sqlglot_dialect = sqlglot_dialect
        self.output_column_name = (
            output_column_name or self._default_output_column_name()
        )
        self.comparison_description = (
            comparison_description or self._default_comparison_description()
        )

        # Assign comparison vector values starting at highest level, count down to 0
        num_levels = self._num_levels
        counter = num_levels - 1

        default_m_values = _default_m_values(num_levels)
        default_u_values = _default_u_values(num_levels)
        for level in self.comparison_levels:
            if level.is_null_level:
                level._comparison_vector_value = -1
                level._max_level = False
            else:
                level._comparison_vector_value = counter
                if counter == num_levels - 1:
                    level._max_level = True
                else:
                    level._max_level = False
                counter -= 1
            level.default_m_probability = default_m_values[
                level.comparison_vector_value
            ]
            level.default_u_probability = default_u_values[
                level.comparison_vector_value
            ]

    @property
    def _num_levels(self):
        return len([cl for cl in self.comparison_levels if not cl.is_null_level])

    @property
    def _comparison_levels_excluding_null(self):
        return [cl for cl in self.comparison_levels if not cl.is_null_level]

    @property
    def column_info_settings(self) -> ColumnInfoSettings:
        if (column_info_settings := self._column_info_settings) is None:
            raise AttributeError(f"No column_info_settings set on Comparison {self}")
        return column_info_settings

    @column_info_settings.setter
    def column_info_settings(self, column_info_settings: ColumnInfoSettings) -> None:
        self._column_info_settings = column_info_settings

    @property
    def gamma_prefix(self):
        return self.column_info_settings.comparison_vector_value_column_prefix

    @property
    def _bf_column_name(self):
        bf_prefix = self.column_info_settings.bayes_factor_column_prefix
        return f"{bf_prefix}{self.output_column_name}".replace(" ", "_")

    @property
    def _has_null_level(self):
        return any([cl.is_null_level for cl in self.comparison_levels])

    @property
    def _bf_tf_adj_column_name(self):
        bf = self.column_info_settings.bayes_factor_column_prefix
        tf = self.column_info_settings.term_frequency_adjustment_column_prefix
        cc_name = self.output_column_name
        return f"{bf}{tf}adj_{cc_name}".replace(" ", "_")

    @property
    def _has_tf_adjustments(self):
        return any([cl._has_tf_adjustments for cl in self.comparison_levels])

    @property
    def _case_statement(self):
        sqls = [
            cl._when_then_comparison_vector_value_sql for cl in self.comparison_levels
        ]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END as {self._gamma_column_name}"

        return sql

    @property
    def _input_columns_used_by_case_statement(self):
        cols = []
        for cl in self.comparison_levels:
            cols.extend(cl._input_columns_used_by_sql_condition)

        # dedupe_preserving_order on input column
        already_observed = []
        deduped_cols = []
        for col in cols:
            if col.input_name not in already_observed:
                deduped_cols.append(col)
                already_observed.append(col.input_name)

        return deduped_cols

    def _default_output_column_name(self):
        cols = self._input_columns_used_by_case_statement
        cols = [c.input_name for c in cols]
        if len(cols) == 1:
            return cols[0]
        return f"custom_{'_'.join(cols)}"

    def _default_comparison_description(self):
        return self.output_column_name

    @property
    def _gamma_column_name(self):
        return f"{self.gamma_prefix}{self.output_column_name}".replace(" ", "_")

    @property
    def _tf_adjustment_input_col_names(self):
        cols = [cl._tf_adjustment_input_column_name for cl in self.comparison_levels]
        cols = [c for c in cols if c]

        return cols

    def _columns_to_select_for_blocking(self):
        cols = []
        for cl in self.comparison_levels:
            cols.extend(cl._columns_to_select_for_blocking)

        return dedupe_preserving_order(cols)

    def _columns_to_select_for_comparison_vector_values(self, retain_matching_columns):
        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        if retain_matching_columns:
            for col in input_cols:
                output_cols.extend(col.names_l_r)

        output_cols.append(self._case_statement)

        for cl in self.comparison_levels:
            if cl._has_tf_adjustments:
                col = cl._tf_adjustment_input_column
                output_cols.extend(col.tf_name_l_r)

        return dedupe_preserving_order(output_cols)

    def _columns_to_select_for_bayes_factor_parts(
        self,
        retain_matching_columns: bool,
        retain_intermediate_calculation_columns: bool,
    ) -> List[str]:
        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        if retain_matching_columns:
            for col in input_cols:
                output_cols.extend(col.names_l_r)

        output_cols.append(self._gamma_column_name)

        if retain_intermediate_calculation_columns:
            for cl in self.comparison_levels:
                if cl._has_tf_adjustments:
                    col = cl._tf_adjustment_input_column
                    output_cols.extend(col.tf_name_l_r)

        # Bayes factor case when statement
        sqls = [
            cl._bayes_factor_sql(self._gamma_column_name)
            for cl in self.comparison_levels
        ]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END as {self._bf_column_name} "
        output_cols.append(sql)

        # tf adjustment case when statement

        if self._has_tf_adjustments:
            sqls = [
                cl._tf_adjustment_sql(self._gamma_column_name, self.comparison_levels)
                for cl in self.comparison_levels
            ]
            sql = " ".join(sqls)
            sql = f"CASE {sql} END as {self._bf_tf_adj_column_name} "
            output_cols.append(sql)
        output_cols.append(self._gamma_column_name)

        return dedupe_preserving_order(output_cols)

    def _columns_to_select_for_predict(
        self,
        retain_matching_columns: bool,
        retain_intermediate_calculation_columns: bool,
        training_mode: bool,
    ) -> List[str]:
        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        if retain_matching_columns:
            for col in input_cols:
                output_cols.extend(col.names_l_r)
            output_cols.append(self._gamma_column_name)
        elif training_mode:
            output_cols.append(self._gamma_column_name)

        if retain_intermediate_calculation_columns:
            for cl in self.comparison_levels:
                if cl._has_tf_adjustments:
                    col = cl._tf_adjustment_input_column
                    output_cols.extend(col.tf_name_l_r)

            output_cols.extend(self._match_weight_columns_to_multiply)

        return dedupe_preserving_order(output_cols)

    @property
    def _match_weight_columns_to_multiply(self):
        cols = []
        cols.append(self._bf_column_name)
        if self._has_tf_adjustments:
            cols.append(self._bf_tf_adj_column_name)
        return cols

    def as_dict(self):
        d = {
            "output_column_name": self.output_column_name,
            "comparison_levels": [cl.as_dict() for cl in self.comparison_levels],
        }
        d["comparison_description"] = self.comparison_description
        return d

    def _as_completed_dict(self):
        return {
            "column_name": self.output_column_name,
            "comparison_levels": [
                cl._as_completed_dict() for cl in self.comparison_levels
            ],
            "input_columns_used_by_case_statement": [
                c.input_name for c in self._input_columns_used_by_case_statement
            ],
        }

    @property
    def _has_estimated_m_values(self):
        return all(cl._has_estimated_m_values for cl in self.comparison_levels)

    @property
    def _has_estimated_u_values(self):
        return all(cl._has_estimated_u_values for cl in self.comparison_levels)

    @property
    def _all_m_are_trained(self):
        return all(cl._m_is_trained for cl in self.comparison_levels)

    @property
    def _all_u_are_trained(self):
        return all(cl._u_is_trained for cl in self.comparison_levels)

    @property
    def _some_m_are_trained(self):
        return any(cl._m_is_trained for cl in self._comparison_levels_excluding_null)

    @property
    def _some_u_are_trained(self):
        return any(cl._u_is_trained for cl in self._comparison_levels_excluding_null)

    @property
    def _is_trained_message(self):
        messages = []
        if self._all_m_are_trained and self._all_u_are_trained:
            return None

        if not self._some_u_are_trained:
            messages.append("no u values are trained")
        elif self._some_u_are_trained and not self._all_u_are_trained:
            messages.append("some u values are not trained")

        if not self._some_m_are_trained:
            messages.append("no m values are trained")
        elif self._some_m_are_trained and not self._all_m_are_trained:
            messages.append("some m values are not trained")

        message = ", ".join(messages)
        message = f"    - {self.output_column_name} ({message})."
        return message

    @property
    def _is_trained(self):
        return self._all_m_are_trained and self._all_u_are_trained

    @property
    def _as_detailed_records(self) -> list[dict[str, Any]]:
        records = []
        for cl in self.comparison_levels:
            record = {}
            record["comparison_name"] = self.output_column_name
            record = {
                **record,
                **cl._as_detailed_record(self._num_levels, self.comparison_levels),
            }
            records.append(record)
        return records

    @property
    def _parameter_estimates_as_records(self):
        records = []
        for cl in self.comparison_levels:
            new_records = cl._parameter_estimates_as_records(
                self._num_levels, self.comparison_levels
            )
            for r in new_records:
                r["comparison_name"] = self.output_column_name
            records.extend(new_records)

        return records

    def _get_comparison_level_by_comparison_vector_value(
        self, value: int
    ) -> ComparisonLevel:
        for cl in self.comparison_levels:
            if cl.comparison_vector_value == value:
                return cl
        raise ValueError(f"No comparison level with comparison vector value {value}")

    def __repr__(self):
        return (
            f"<Comparison {self.comparison_description} with "
            f"{self._num_levels} levels at {hex(id(self))}>"
        )

    @property
    def _not_trained_messages(self):
        msgs = []

        cname = self.output_column_name

        header = f"Comparison: '{cname}':\n"

        msg_template = "{header}    {m_or_u} values not fully trained"

        if not self._all_m_are_trained:
            msgs.append(msg_template.format(header=header, m_or_u="m"))
        if not self._all_u_are_trained:
            msgs.append(msg_template.format(header=header, m_or_u="u"))

        return msgs

    @property
    def _comparison_level_description_list(self):
        cl_template = "    - '{label}' with SQL rule: {sql}\n"

        comp_levels = [
            cl_template.format(
                cvv=cl.comparison_vector_value,
                label=cl.label_for_charts,
                sql=re.sub(r"\s+", " ", cl.sql_condition.replace("\n", " ")),
            )
            for cl in self.comparison_levels
        ]
        comp_levels_str = "".join(comp_levels)
        return comp_levels_str

    @property
    def _human_readable_description_succinct(self):
        input_cols = join_list_with_commas_final_and(
            [c.name for c in self._input_columns_used_by_case_statement]
        )

        comp_levels = self._comparison_level_description_list

        main_desc = f"of {input_cols}\nDescription: '{self.comparison_description}'"

        desc = f"Comparison {main_desc}\nComparison levels:\n{comp_levels}"
        return desc

    @property
    def human_readable_description(self):
        input_cols = join_list_with_commas_final_and(
            [c.name for c in self._input_columns_used_by_case_statement]
        )

        comp_levels = self._comparison_level_description_list

        main_desc = f"'{self.comparison_description}' of {input_cols}"

        desc = (
            f"Comparison {main_desc}.\n"
            "Similarity is assessed using the following "
            f"ComparisonLevels:\n{comp_levels}"
        )

        return desc

    def match_weights_chart(self, as_dict=False):
        """Display a chart of comparison levels of the comparison"""
        from splink.internals.charts import comparison_match_weights_chart

        records = self._as_detailed_records
        return comparison_match_weights_chart(records, as_dict=as_dict)

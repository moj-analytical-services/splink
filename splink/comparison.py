from typing import TYPE_CHECKING, List

from .comparison_level import ComparisonLevel
from .misc import dedupe_preserving_order, join_list_with_commas_final_and


# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .settings import Settings


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

    def __init__(self, comparison_dict, settings_obj: "Settings" = None):

        # Protected because we don't want to modify
        self._comparison_dict = comparison_dict
        comparison_level_list = comparison_dict["comparison_levels"]
        self.comparison_levels: List[ComparisonLevel] = []

        # If comparison_levels are already of type ComparisonLevel, register
        # the settings object on them
        # otherwise turn the dictionaries into ComparisonLevel

        for cl in comparison_level_list:
            if isinstance(cl, ComparisonLevel):
                cl.comparison = self
            elif settings_obj is None:
                cl = ComparisonLevel(cl, self)
            else:
                cl = ComparisonLevel(cl, self, sql_dialect=settings_obj._sql_dialect)

            self.comparison_levels.append(cl)

        self._settings_obj: "Settings" = settings_obj

        # Assign comparison vector values starting at highest level, count down to 0
        num_levels = self._num_levels
        counter = num_levels - 1

        for level in self.comparison_levels:
            if level._is_null_level:
                level._comparison_vector_value = -1
                level._max_level = False
            else:
                level._comparison_vector_value = counter
                if counter == num_levels - 1:
                    level._max_level = True
                else:
                    level._max_level = False
                counter -= 1

    def __deepcopy__(self, memo):
        """When we do EM training, we need a copy of the Comparison which is independent
        of the original e.g. modifying the copy will not affect the original.
        This method implements ensures the Comparison can be deepcopied.
        """
        cc = Comparison(self.as_dict(), self._settings_obj)
        return cc

    @property
    def _num_levels(self):
        return len([cl for cl in self.comparison_levels if not cl._is_null_level])

    @property
    def _comparison_levels_excluding_null(self):
        return [cl for cl in self.comparison_levels if not cl._is_null_level]

    @property
    def _gamma_prefix(self):
        return self._settings_obj._gamma_prefix

    @property
    def _retain_intermediate_calculation_columns(self):
        return self._settings_obj._retain_intermediate_calculation_columns

    @property
    def _bf_column_name(self):
        return f"{self._settings_obj._bf_prefix}{self._output_column_name}".replace(
            " ", "_"
        )

    @property
    def _has_null_level(self):
        return any([cl._is_null_level for cl in self.comparison_levels])

    @property
    def _bf_tf_adj_column_name(self):
        bf = self._settings_obj._bf_prefix
        tf = self._settings_obj._tf_prefix
        cc_name = self._output_column_name
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

    @property
    def _output_column_name(self):
        if "output_column_name" in self._comparison_dict:
            return self._comparison_dict["output_column_name"]
        else:
            cols = self._input_columns_used_by_case_statement
            cols = [c.input_name for c in cols]
            if len(cols) == 1:
                return cols[0]
            else:
                return f"custom_{'_'.join(cols)}"

    @property
    def _comparison_description(self):
        if "comparison_description" in self._comparison_dict:
            return self._comparison_dict["comparison_description"]
        else:
            return self._output_column_name

    @property
    def _gamma_column_name(self):
        return f"{self._gamma_prefix}{self._output_column_name}".replace(" ", "_")

    @property
    def _tf_adjustment_input_col_names(self):
        cols = [cl._tf_adjustment_input_column_name for cl in self.comparison_levels]
        cols = [c for c in cols if c]

        return cols

    @property
    def _columns_to_select_for_blocking(self):
        cols = []
        for cl in self.comparison_levels:
            cols.extend(cl._columns_to_select_for_blocking)

        return dedupe_preserving_order(cols)

    @property
    def _columns_to_select_for_comparison_vector_values(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self._settings_obj._retain_matching_columns:
                output_cols.extend(col.names_l_r())

        output_cols.append(self._case_statement)

        for col in input_cols:

            if self._has_tf_adjustments:
                output_cols.extend(col.tf_name_l_r())

        return dedupe_preserving_order(output_cols)

    @property
    def _columns_to_select_for_bayes_factor_parts(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self._settings_obj._retain_matching_columns:

                output_cols.extend(col.names_l_r())

        output_cols.append(self._gamma_column_name)

        for col in input_cols:

            if self._has_tf_adjustments:
                if self._settings_obj._retain_intermediate_calculation_columns:

                    output_cols.extend(col.tf_name_l_r())

        # Bayes factor case when statement
        sqls = [cl._bayes_factor_sql for cl in self.comparison_levels]
        sql = " ".join(sqls)
        sql = f"CASE {sql} END as {self._bf_column_name} "
        output_cols.append(sql)

        # tf adjustment case when statement

        if self._has_tf_adjustments:
            sqls = [cl._tf_adjustment_sql for cl in self.comparison_levels]
            sql = " ".join(sqls)
            sql = f"CASE {sql} END as {self._bf_tf_adj_column_name} "
            output_cols.append(sql)
        output_cols.append(self._gamma_column_name)

        return dedupe_preserving_order(output_cols)

    @property
    def _columns_to_select_for_predict(self):

        input_cols = []
        for cl in self.comparison_levels:
            input_cols.extend(cl._input_columns_used_by_sql_condition)

        output_cols = []
        for col in input_cols:
            if self._settings_obj._retain_matching_columns:
                output_cols.extend(col.names_l_r())

        if (
            self._settings_obj._training_mode
            or self._settings_obj._retain_matching_columns
        ):
            output_cols.append(self._gamma_column_name)

        for col in input_cols:
            if self._settings_obj._retain_intermediate_calculation_columns:
                if self._has_tf_adjustments:

                    output_cols.extend(col.tf_name_l_r())

                output_cols.extend(self._match_weight_columns_to_multiply)

        return dedupe_preserving_order(output_cols)

    @property
    def _match_weight_columns_to_multiply(self):
        cols = []
        cols.append(self._bf_column_name)
        if self._has_tf_adjustments:
            cols.append(self._bf_tf_adj_column_name)
        return cols

    @property
    def _term_frequency_columns(self):
        cols = set()
        for cl in self.comparison_levels:
            cols.add(cl.tf_adjustment_input_col_name)
        return list(cols)

    def as_dict(self):
        d = {
            "output_column_name": self._output_column_name,
            "comparison_levels": [cl.as_dict() for cl in self.comparison_levels],
        }
        if "comparison_description" in self._comparison_dict:
            d["comparison_description"] = self._comparison_dict[
                "comparison_description"
            ]
        return d

    def _as_completed_dict(self):
        return {
            "column_name": self._output_column_name,
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
        message = f"    - {self._output_column_name} ({message})."
        return message

    @property
    def _is_trained(self):
        return self._all_m_are_trained and self._all_u_are_trained

    @property
    def _as_detailed_records(self):
        records = []
        for cl in self.comparison_levels:
            record = {}
            record["comparison_name"] = self._output_column_name
            record = {**record, **cl._as_detailed_record}
            records.append(record)
        return records

    @property
    def _parameter_estimates_as_records(self):
        records = []
        for cl in self.comparison_levels:
            new_records = cl._parameter_estimates_as_records
            for r in new_records:
                r["comparison_name"] = self._output_column_name
            records.extend(new_records)

        return records

    def _get_comparison_level_by_comparison_vector_value(
        self, value
    ) -> ComparisonLevel:
        for cl in self.comparison_levels:

            if cl._comparison_vector_value == value:
                return cl
        raise ValueError(f"No comparison level with comparison vector value {value}")

    def __repr__(self):
        return (
            f"<Comparison {self._comparison_description} with "
            f"{self._num_levels} levels at {hex(id(self))}>"
        )

    @property
    def _not_trained_messages(self):

        msgs = []

        cname = self._output_column_name

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
                cvv=cl._comparison_vector_value,
                label=cl._label_for_charts,
                sql=cl._sql_condition,
            )
            for cl in self.comparison_levels
        ]
        comp_levels = "".join(comp_levels)
        return comp_levels

    @property
    def _human_readable_description_succinct(self):

        input_cols = join_list_with_commas_final_and(
            [c.name(escape=False) for c in self._input_columns_used_by_case_statement]
        )

        comp_levels = self._comparison_level_description_list

        if "comparison_description" in self._comparison_dict:
            main_desc = (
                f"of {input_cols}\nDescription: '{self._comparison_description}'"
            )
        else:
            main_desc = f"of {input_cols}"

        desc = f"Comparison {main_desc}\nComparison levels:\n{comp_levels}"
        return desc

    @property
    def human_readable_description(self):

        input_cols = join_list_with_commas_final_and(
            [c.name(escape=False) for c in self._input_columns_used_by_case_statement]
        )

        comp_levels = self._comparison_level_description_list

        if "comparison_description" in self._comparison_dict:
            main_desc = f"'{self._comparison_description}' of {input_cols}"
        else:
            main_desc = f"of {input_cols}"

        desc = (
            f"Comparison {main_desc}.\n"
            "Similarity is assessed using the following "
            f"ComparisonLevels:\n{comp_levels}"
        )

        return desc

    def match_weights_chart(self, as_dict=False):
        """Display a chart of comparison levels of the comparison"""
        from .charts import comparison_match_weights_chart

        records = self._as_detailed_records
        return comparison_match_weights_chart(records, as_dict=as_dict)

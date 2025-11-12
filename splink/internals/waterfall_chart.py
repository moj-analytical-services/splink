from __future__ import annotations

import math
from copy import deepcopy
from typing import Any, Dict

from splink.internals.comparison import Comparison
from splink.internals.misc import prob_to_bayes_factor


def _prior_record(settings_obj):
    rec: Dict[str, Any] = {}
    rec["column_name"] = "Prior"
    rec["label_for_charts"] = "Starting match weight (prior)"
    rec["sql_condition"] = None
    bf = prob_to_bayes_factor(settings_obj._probability_two_random_records_match)
    rec["log2_bayes_factor"] = math.log2(bf)
    rec["bayes_factor"] = bf
    rec["comparison_vector_value"] = None
    rec["m_probability"] = None
    rec["u_probability"] = None
    rec["bayes_factor_description"] = None
    rec["value_l"] = ""
    rec["value_r"] = ""
    rec["term_frequency_adjustment"] = None
    return rec


def _final_score_record(record_as_dict):
    rec: Dict[str, Any] = {}
    rec["column_name"] = "Final score"
    rec["label_for_charts"] = "Final score"
    rec["sql_condition"] = None
    rec["log2_bayes_factor"] = record_as_dict["match_weight"]
    rec["bayes_factor"] = 2 ** record_as_dict["match_weight"]
    rec["comparison_vector_value"] = None
    rec["m_probability"] = None
    rec["u_probability"] = None
    rec["bayes_factor_description"] = None
    rec["value_l"] = ""
    rec["value_r"] = ""
    rec["term_frequency_adjustment"] = None
    return rec


def _comparison_records(
    record_as_dict: dict[str, Any],
    comparison: Comparison,
    hide_details: bool = False,
    _cols_cache: (dict[Comparison, tuple[list[str], list[str]]] | None) = None,
) -> list[dict[str, Any]]:
    output_records = []

    c = comparison
    cv_value = record_as_dict[c._gamma_column_name]

    cl = c._get_comparison_level_by_comparison_vector_value(cv_value)
    waterfall_record = {
        field: value
        for field, value in cl._as_detailed_record(
            c._num_levels, c.comparison_levels
        ).items()
        if field
        in [
            "label_for_charts",
            "sql_condition",
            "log2_bayes_factor",
            "bayes_factor",
            "comparison_vector_value",
            "m_probability",
            "u_probability",
            "bayes_factor_description",
        ]
    }

    waterfall_record["column_name"] = c.output_column_name

    if _cols_cache is not None and c in _cols_cache:
        input_cols_l, input_cols_r = _cols_cache[c]
    else:
        input_cols_used = c._input_columns_used_by_case_statement
        input_cols_l = [ic.unquote().name_l for ic in input_cols_used]
        input_cols_r = [ic.unquote().name_r for ic in input_cols_used]
        if _cols_cache is not None:
            _cols_cache[c] = (input_cols_l, input_cols_r)

    if hide_details:
        waterfall_record["value_l"] = ""
        waterfall_record["value_r"] = ""
    else:
        waterfall_record["value_l"] = ", ".join(
            [str(record_as_dict[n]) for n in input_cols_l]
        )
        waterfall_record["value_r"] = ", ".join(
            [str(record_as_dict[n]) for n in input_cols_r]
        )

    waterfall_record["term_frequency_adjustment"] = False

    output_records.append(waterfall_record)
    # Term frequency record if needed

    if c._has_tf_adjustments:
        waterfall_record_2 = deepcopy(waterfall_record)

        if cl._tf_adjustment_input_column is not None and hide_details is False:
            waterfall_record_2["value_l"] = str(
                record_as_dict[cl._tf_adjustment_input_column.unquote().name_l]
            )
            waterfall_record_2["value_r"] = str(
                record_as_dict[cl._tf_adjustment_input_column.unquote().name_r]
            )
        else:
            waterfall_record_2["value_l"] = ""
            waterfall_record_2["value_r"] = ""

        waterfall_record_2["column_name"] = "tf_" + c.output_column_name
        waterfall_record_2["term_frequency_adjustment"] = True
        waterfall_record_2["bayes_factor"] = 1.0
        waterfall_record_2["log2_bayes_factor"] = math.log2(1.0)
        if cl._has_tf_adjustments:
            waterfall_record_2["label_for_charts"] = (
                f"Term freq adjustment on {cl._tf_adjustment_input_column.input_name}"
            )
            bf = record_as_dict[c._bf_tf_adj_column_name]
            waterfall_record_2["bayes_factor"] = bf
            waterfall_record_2["log2_bayes_factor"] = math.log2(bf)
            waterfall_record_2["m_probability"] = None
            waterfall_record_2["u_probability"] = None
            waterfall_record_2["bayes_factor_description"] = None

            text = (
                "Term frequency adjustment on "
                f"{cl._tf_adjustment_input_column.input_name} makes comparison"
            )
            if bf >= 1.0:
                text = f"{text} {bf:,.2f} times more likely to be a match"
            else:
                mult = 1 / bf
                text = f"{text}  {mult:,.2f} times less likely to be a match"

            waterfall_record_2["bayes_factor_description"] = text

        output_records.append(waterfall_record_2)

    return output_records


def record_to_waterfall_data(
    record_as_dict, settings_obj, hide_details, _cols_cache=None
):
    comparisons = settings_obj.comparisons
    waterfall_records = [_prior_record(settings_obj)]

    for cc in comparisons:
        records = _comparison_records(
            record_as_dict, cc, hide_details, _cols_cache=_cols_cache
        )
        waterfall_records.extend(records)

    waterfall_records.append(_final_score_record(record_as_dict))
    for i, rec in enumerate(waterfall_records):
        rec["bar_sort_order"] = i
    return waterfall_records


def records_to_waterfall_data(records, settings_obj, hide_details):
    # The cache is makes this dramatically faster because
    # c._input_columns_used_by_case_statement is expensive.
    # Without the cache it is called for every record within
    # record_to_waterfall_data
    cols_cache = {}
    for c in settings_obj.comparisons:
        used = c._input_columns_used_by_case_statement
        cols_cache[c] = (
            [ic.unquote().name_l for ic in used],
            [ic.unquote().name_r for ic in used],
        )

    waterfall_data = []
    for i, record in enumerate(records):
        new_data = record_to_waterfall_data(
            record, settings_obj, hide_details, _cols_cache=cols_cache
        )
        for rec in new_data:
            rec["record_number"] = i
        waterfall_data.extend(new_data)

    return waterfall_data

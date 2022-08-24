import math
from copy import deepcopy

from .misc import prob_to_bayes_factor
from .comparison import Comparison


def _prior_record(settings_obj):
    rec = {}
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
    rec = {}
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


def _comparison_records(record_as_dict, comparison: Comparison):

    output_records = []
    waterfall_record = {}

    c = comparison
    cv_value = record_as_dict[c._gamma_column_name]

    cl = c._get_comparison_level_by_comparison_vector_value(cv_value)

    waterfall_record["column_name"] = c._output_column_name
    waterfall_record["label_for_charts"] = cl._label_for_charts

    waterfall_record["sql_condition"] = cl._sql_condition
    waterfall_record["log2_bayes_factor"] = cl._log2_bayes_factor
    waterfall_record["bayes_factor"] = cl._bayes_factor
    waterfall_record["comparison_vector_value"] = int(cv_value)
    waterfall_record["m_probability"] = cl.m_probability
    waterfall_record["u_probability"] = cl.u_probability
    waterfall_record["bayes_factor_description"] = cl._bayes_factor_description
    input_cols_used = c._input_columns_used_by_case_statement
    input_cols_l = [ic.unquote().name_l() for ic in input_cols_used]
    input_cols_r = [ic.unquote().name_r() for ic in input_cols_used]
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

        if cl._tf_adjustment_input_column is not None:
            waterfall_record_2["value_l"] = str(
                record_as_dict[cl._tf_adjustment_input_column.unquote().name_l()]
            )
            waterfall_record_2["value_r"] = str(
                record_as_dict[cl._tf_adjustment_input_column.unquote().name_r()]
            )
        else:
            waterfall_record_2["value_l"] = ""
            waterfall_record_2["value_r"] = ""

        waterfall_record_2["column_name"] = "tf_" + c._output_column_name
        waterfall_record_2["term_frequency_adjustment"] = True
        waterfall_record_2["bayes_factor"] = 1.0
        waterfall_record_2["log2_bayes_factor"] = math.log2(1.0)
        if cl._has_tf_adjustments:
            waterfall_record_2["label_for_charts"] = (
                f"Term freq adjustment on {cl._tf_adjustment_input_column.input_name} "
                "with weight {cl.tf_adjustment_weight}"
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


def record_to_waterfall_data(record_as_dict, settings_obj):
    comparisons = settings_obj.comparisons
    waterfall_records = [_prior_record(settings_obj)]

    for cc in comparisons:
        records = _comparison_records(record_as_dict, cc)
        waterfall_records.extend(records)

    waterfall_records.append(_final_score_record(record_as_dict))
    for i, rec in enumerate(waterfall_records):
        rec["bar_sort_order"] = i
    return waterfall_records


def records_to_waterfall_data(records, settings_obj):
    waterfall_data = []
    for i, record in enumerate(records):

        new_data = record_to_waterfall_data(record, settings_obj)
        for rec in new_data:
            rec["record_number"] = i
        waterfall_data.extend(new_data)

    return waterfall_data

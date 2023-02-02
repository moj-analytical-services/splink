import logging

from .comparison_level import ComparisonLevel
from .constants import LEVEL_NOT_OBSERVED_TEXT

logger = logging.getLogger(__name__)


def m_u_records_to_lookup_dict(m_u_records):
    lookup = {}
    for m_u_record in m_u_records:
        comparison_name = m_u_record["output_column_name"]
        level_value = m_u_record["comparison_vector_value"]
        if comparison_name not in lookup:
            lookup[comparison_name] = {}
        if level_value not in lookup[comparison_name]:
            lookup[comparison_name][level_value] = {}

        m_prob = m_u_record["m_probability"]

        u_prob = m_u_record["u_probability"]

        if m_prob is not None:
            lookup[comparison_name][level_value]["m_probability"] = m_prob
        if u_prob is not None:
            lookup[comparison_name][level_value]["u_probability"] = u_prob

    return lookup


def not_trained_message(comparison_level: ComparisonLevel):
    c = comparison_level.comparison
    cl = comparison_level
    return (
        f"not trained for {c._output_column_name} - "
        f"{cl._label_for_charts} (comparison vector value: "
        f"{cl._comparison_vector_value}). This usually means the "
        "comparison level was never observed in the training data."
    )


def append_u_probability_to_comparison_level_trained_probabilities(
    comparison_level: ComparisonLevel, m_u_records_lookup, training_description
):

    cl = comparison_level
    c = cl.comparison

    try:
        u_probability = m_u_records_lookup[c._output_column_name][
            cl._comparison_vector_value
        ]["u_probability"]

    except KeyError:
        u_probability = LEVEL_NOT_OBSERVED_TEXT

        logger.info(f"u probability {not_trained_message(cl)}")
    cl._add_trained_u_probability(
        u_probability,
        training_description,
    )


def append_m_probability_to_comparison_level_trained_probabilities(
    comparison_level: ComparisonLevel, m_u_records_lookup, training_description
):
    cl = comparison_level
    c = cl.comparison

    try:
        m_probability = m_u_records_lookup[c._output_column_name][
            cl._comparison_vector_value
        ]["m_probability"]

    except KeyError:
        m_probability = LEVEL_NOT_OBSERVED_TEXT

        logger.info(f"m probability {not_trained_message(cl)}")
    cl._add_trained_m_probability(
        m_probability,
        training_description,
    )

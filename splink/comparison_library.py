from typing import Union
from .comparison import Comparison
from . import comparison_level_library as cl
from .misc import ensure_is_iterable


def exact_match(
    col_name,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_else=None,
) -> Comparison:
    comparison_dict = {
        "comparison_description": "Exact match vs. anything else",
        "comparison_levels": [
            cl.null_level(col_name),
            cl.exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            ),
            cl.else_level(m_probability=m_probability_else),
        ],
    }
    return Comparison(comparison_dict)


def levenshtein(
    col_name: str,
    distance_threshold_or_thresholds: Union[int, list] = 2,
    exact_match_level=True,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_or_probabilities_lev: Union[float, list] = None,
    m_probability_else=None,
) -> Comparison:

    distance_thresholds = ensure_is_iterable(distance_threshold_or_thresholds)

    if m_probability_or_probabilities_lev is None:
        m_probability_or_probabilities_lev = [None] * len(distance_thresholds)
    m_probabilities = ensure_is_iterable(m_probability_or_probabilities_lev)

    comparison_levels = []
    comparison_levels.append(cl.null_level(col_name))
    if exact_match_level:
        level = cl.exact_match_level(
            col_name,
            term_frequency_adjustments=term_frequency_adjustments,
            m_probability=m_probability_exact_match,
        )
        comparison_levels.append(level)

    for thres, m_prob in zip(distance_thresholds, m_probabilities):
        level = cl.levenshtein_level(
            col_name,
            distance_threshold=thres,
            m_probability=m_prob,
        )
        comparison_levels.append(level)

    comparison_levels.append(
        cl.else_level(m_probability=m_probability_else),
    )

    comparison_desc = ""
    if exact_match_level:
        comparison_desc += "Exact match vs. "

    thres_desc = ", ".join([str(d) for d in distance_thresholds])
    plural = "" if len(distance_thresholds) == 1 else "s"
    comparison_desc += f"Levenstein at threshold{plural} {thres_desc} vs. "
    comparison_desc += "anything else"

    comparison_dict = {
        "comparison_description": comparison_desc,
        "comparison_levels": comparison_levels,
    }
    return Comparison(comparison_dict)

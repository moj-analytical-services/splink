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
    """A comparison of the data in `col_name` with two levels:
    - Exact match
    - Anything else

    Args:
        col_name (str): The name of the column to compare
        term_frequency_adjustments (bool, optional): If True, term frequency adjustments
            will be made on the exact match level. Defaults to False.
        m_probability_exact_match (_type_, optional): If provided, overrides the
            default m probability for the exact match level. Defaults to None.
        m_probability_else (_type_, optional): If provided, overrides the
            default m probability for the 'anything else' level. Defaults to None.

    Returns:
        Comparison: A comparison that can be inclued in the Splink settings dictionary
    """

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


def distance_function_at_thresholds(
    col_name: str,
    distance_function_name: str,
    distance_threshold_or_thresholds: Union[int, list],
    higher_is_more_similar: bool = True,
    include_exact_match_level=True,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_or_probabilities_lev: Union[float, list] = None,
    m_probability_else=None,
) -> Comparison:
    """A comparison of the data in `col_name` with a user-provided distance function
    used to assess middle similarity levels.

    The user-provided distance function must exist in the SQL backend.

    An example of the output with default arguments and setting `distance_function_name`
    to `jaccard` and `distance_threshold_or_thresholds = [0.9,0.7]` would be
    - Exact match
    - Jaccard distance <= 0.9
    - Jaccard distance <= 0.7
    - Anything else

    Args:
        col_name (str): The name of the column to compare
        distance_function_name (str): The name of the distance function
        distance_threshold_or_thresholds (Union[int, list], optional): The threshold(s)
            to use for the middle similarity level(s). Defaults to [1, 2].
        higher_is_more_similar (bool): If True, a higher value of the distance function
            indicates a higher similarity (e.g. jaro_winkler).  If false, a higher
            value indicates a lower similarity (e.g. levenshtein).
        include_exact_match_level (bool, optional): If True, include an exact match
            level. Defaults to True.
        term_frequency_adjustments (bool, optional): If True, apply term frequency
            adjustments to the exact match level. Defaults to False.
        m_probability_exact_match (_type_, optional): If provided, overrides the
            default m probability for the exact match level. Defaults to None.
        m_probability_or_probabilities_lev (Union[float, list], optional):
            _description_. If provided, overrides the default m probabilities
            for the thresholds specified. Defaults to None.
        m_probability_else (_type_, optional): If provided, overrides the
            default m probability for the 'anything else' level. Defaults to None.

    Returns:
        Comparison:
    """

    distance_thresholds = ensure_is_iterable(distance_threshold_or_thresholds)

    if m_probability_or_probabilities_lev is None:
        m_probability_or_probabilities_lev = [None] * len(distance_thresholds)
    m_probabilities = ensure_is_iterable(m_probability_or_probabilities_lev)

    comparison_levels = []
    comparison_levels.append(cl.null_level(col_name))
    if include_exact_match_level:
        level = cl.exact_match_level(
            col_name,
            term_frequency_adjustments=term_frequency_adjustments,
            m_probability=m_probability_exact_match,
        )
        comparison_levels.append(level)

    for thres, m_prob in zip(distance_thresholds, m_probabilities):
        level = cl.distance_function_level(
            col_name,
            distance_function_name=distance_function_name,
            higher_is_more_similar=higher_is_more_similar,
            distance_threshold=thres,
            m_probability=m_prob,
        )
        comparison_levels.append(level)

    comparison_levels.append(
        cl.else_level(m_probability=m_probability_else),
    )

    comparison_desc = ""
    if include_exact_match_level:
        comparison_desc += "Exact match vs. "

    thres_desc = ", ".join([str(d) for d in distance_thresholds])
    plural = "" if len(distance_thresholds) == 1 else "s"
    comparison_desc += (
        f"{distance_function_name} at threshold{plural} {thres_desc} vs. "
    )
    comparison_desc += "anything else"

    comparison_dict = {
        "comparison_description": comparison_desc,
        "comparison_levels": comparison_levels,
    }
    return Comparison(comparison_dict)


def levenshtein_at_thresholds(
    col_name: str,
    distance_threshold_or_thresholds: Union[int, list] = [1, 2],
    include_exact_match_level=True,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_or_probabilities_lev: Union[float, list] = None,
    m_probability_else=None,
) -> Comparison:
    """A comparison of the data in `col_name` with the levenshtein distance used to
    assess middle similarity levels.

    An example of the output with default arguments and setting
    `distance_threshold_or_thresholds = [1,2]` would be
    - Exact match
    - levenshtein distance <= 1
    - levenshtein distance <= 2
    - Anything else

    Args:
        col_name (str): The name of the column to compare
        distance_threshold_or_thresholds (Union[int, list], optional): The threshold(s)
            to use for the middle similarity level(s). Defaults to [1, 2].
        include_exact_match_level (bool, optional): If True, include an exact match
            level. Defaults to True.
        term_frequency_adjustments (bool, optional): If True, apply term frequency
            adjustments to the exact match level. Defaults to False.
        m_probability_exact_match (_type_, optional): If provided, overrides the
            default m probability for the exact match level. Defaults to None.
        m_probability_or_probabilities_lev (Union[float, list], optional):
            _description_. If provided, overrides the default m probabilities
            for the thresholds specified. Defaults to None.
        m_probability_else (_type_, optional): If provided, overrides the
            default m probability for the 'anything else' level. Defaults to None.

    Returns:
        Comparison:
    """

    return distance_function_at_thresholds(
        col_name,
        cl._mutable_params["levenshtein"],
        distance_threshold_or_thresholds,
        False,
        include_exact_match_level,
        term_frequency_adjustments,
        m_probability_exact_match,
        m_probability_or_probabilities_lev,
        m_probability_else,
    )


def jaccard_at_thresholds(
    col_name: str,
    distance_threshold_or_thresholds: Union[int, list] = [0.9, 0.7],
    include_exact_match_level=True,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_or_probabilities_lev: Union[float, list] = None,
    m_probability_else=None,
) -> Comparison:
    """A comparison of the data in `col_name` with the jaccard distance used to
    assess middle similarity levels.

    An example of the output with default arguments and setting
    `distance_threshold_or_thresholds = [1,2]` would be
    - Exact match
    - Jaccard distance <= 0.9
    - Jaccard distance <= 0.7
    - Anything else

    Args:
        col_name (str): The name of the column to compare
        distance_threshold_or_thresholds (Union[int, list], optional): The threshold(s)
            to use for the middle similarity level(s). Defaults to [0.9, 0.7].
        include_exact_match_level (bool, optional): If True, include an exact match
            level. Defaults to True.
        term_frequency_adjustments (bool, optional): If True, apply term frequency
            adjustments to the exact match level. Defaults to False.
        m_probability_exact_match (_type_, optional): If provided, overrides the
            default m probability for the exact match level. Defaults to None.
        m_probability_or_probabilities_lev (Union[float, list], optional):
            _description_. If provided, overrides the default m probabilities
            for the thresholds specified. Defaults to None.
        m_probability_else (_type_, optional): If provided, overrides the
            default m probability for the 'anything else' level. Defaults to None.

    Returns:
        Comparison:
    """

    return distance_function_at_thresholds(
        col_name,
        "jaccard",
        distance_threshold_or_thresholds,
        True,
        include_exact_match_level,
        term_frequency_adjustments,
        m_probability_exact_match,
        m_probability_or_probabilities_lev,
        m_probability_else,
    )


def jaro_winkler_at_thresholds(
    col_name: str,
    distance_threshold_or_thresholds: Union[int, list] = [0.9, 0.7],
    include_exact_match_level=True,
    term_frequency_adjustments=False,
    m_probability_exact_match=None,
    m_probability_or_probabilities_lev: Union[float, list] = None,
    m_probability_else=None,
) -> Comparison:
    """A comparison of the data in `col_name` with the jaro_winkler distance used to
    assess middle similarity levels.

    An example of the output with default arguments and setting
    `distance_threshold_or_thresholds = [1,2]` would be
    - Exact match
    - jaro_winkler distance <= 0.9
    - jaro_winkler distance <= 0.7
    - Anything else

    Args:
        col_name (str): The name of the column to compare
        distance_threshold_or_thresholds (Union[int, list], optional): The threshold(s)
            to use for the middle similarity level(s). Defaults to [0.9, 0.7].
        include_exact_match_level (bool, optional): If True, include an exact match
            level. Defaults to True.
        term_frequency_adjustments (bool, optional): If True, apply term frequency
            adjustments to the exact match level. Defaults to False.
        m_probability_exact_match (_type_, optional): If provided, overrides the
            default m probability for the exact match level. Defaults to None.
        m_probability_or_probabilities_lev (Union[float, list], optional):
            _description_. If provided, overrides the default m probabilities
            for the thresholds specified. Defaults to None.
        m_probability_else (_type_, optional): If provided, overrides the
            default m probability for the 'anything else' level. Defaults to None.

    Returns:
        Comparison:
    """

    return distance_function_at_thresholds(
        col_name,
        "jaro_winkler",
        distance_threshold_or_thresholds,
        True,
        include_exact_match_level,
        term_frequency_adjustments,
        m_probability_exact_match,
        m_probability_or_probabilities_lev,
        m_probability_else,
    )

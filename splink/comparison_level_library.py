from typing import Union

from .input_column import InputColumn
from .comparison_level import ComparisonLevel


# So that we can pass in a sql_dialect to the comparison levels
# that is different depending on whether we're in a SparkComparionLevel,
# DuckDBComparisonLevel etc.
_mutable_params = {"dialect": None, "levenshtein": "levenshtein"}


def distance_function_level(
    col_name: str,
    distance_function_name: str,
    distance_threshold: Union[int, float],
    higher_is_more_similar: bool = True,
    m_probability=None,
) -> ComparisonLevel:
    """Represents a comparison using a user-provided distance function,
    where the similarity

    Args:
        col_name (str): Input column name
        distance_function_name (str): The name of the distance function
        distance_threshold (Union[int, float]): The threshold to use to assess
            similarity
        higher_is_more_similar (bool): If True, a higher value of the distance function
            indicates a higher similarity (e.g. jaro_winkler).  If false, a higher
            value indicates a lower similarity (e.g. levenshtein).
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.

    Returns:
        ComparisonLevel:
    """
    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])

    if higher_is_more_similar:
        operator = ">="
    else:
        operator = "<="

    sql_cond = (
        f"{distance_function_name}({col.name_l()}, {col.name_r()}) "
        f"{operator} {distance_threshold}"
    )
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{distance_function_name} {operator} {distance_threshold}",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])


def null_level(col_name) -> ComparisonLevel:
    """Represents comparisons where one or both sides of the comparison
    contains null values so the similarity cannot be evaluated.

    Assumed to have a partial match weight of zero (null effect on overall match weight)

    Args:
        col_name (str): Input column name

    Returns:
        ComparisonLevel: Comparison level
    """

    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])
    level_dict = {
        "sql_condition": f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL",
        "label_for_charts": "Null",
        "is_null_level": True,
    }
    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])


def exact_match_level(
    col_name, m_probability=None, term_frequency_adjustments=False
) -> ComparisonLevel:

    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])
    level_dict = {
        "sql_condition": f"{col.name_l()} = {col.name_r()}",
        "label_for_charts": "Exact match",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability
    if term_frequency_adjustments:
        level_dict["tf_adjustment_column"] = col_name

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])


def levenshtein_level(
    col_name: str,
    distance_threshold: int,
    m_probability=None,
) -> ComparisonLevel:
    """Represents a comparison using a levenshtein distance function,

    Args:
        col_name (str): Input column name
        distance_threshold (Union[int, float]): The threshold to use to assess
            similarity
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.

    Returns:
        ComparisonLevel:
    """
    lev_name = _mutable_params["levenshtein"]
    return distance_function_level(
        col_name,
        lev_name,
        distance_threshold,
        False,
        m_probability=m_probability,
    )


def jaro_winkler_level(
    col_name: str,
    distance_threshold: float,
    m_probability=None,
) -> ComparisonLevel:
    """Represents a comparison using the jaro winkler distance function

    Args:
        col_name (str): Input column name
        distance_threshold (Union[int, float]): The threshold to use to assess
            similarity
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.

    Returns:
        ComparisonLevel: A comparison level that evaluates the jaro winkler similarity
    """
    return distance_function_level(
        col_name,
        "jaro_winkler",
        distance_threshold,
        True,
        m_probability=m_probability,
    )


def jaccard_level(
    col_name: str,
    distance_threshold: Union[int, float],
    m_probability=None,
) -> ComparisonLevel:
    """Represents a comparison using a jaccard distance function

    Args:
        col_name (str): Input column name
        distance_threshold (Union[int, float]): The threshold to use to assess
            similarity
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.

    Returns:
        ComparisonLevel: A comparison level that evaluates the jaccard similarity
    """
    return distance_function_level(
        col_name,
        "jaccard",
        distance_threshold,
        True,
        m_probability=m_probability,
    )


def else_level(
    m_probability=None,
) -> ComparisonLevel:

    if isinstance(m_probability, str):
        raise ValueError(
            "You provided a string for the value of m probability when it should be "
            "numeric.  Perhaps you passed a column name.  Note that you do not need to "
            "pass a column name into the else level."
        )
    level_dict = {
        "sql_condition": "ELSE",
        "label_for_charts": "All other comparisons",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability
    return ComparisonLevel(level_dict)


def columns_reversed_level(
    col_name_1: str, col_name_2: str, m_probability=None, tf_adjustment_column=None
) -> ComparisonLevel:
    """Represents a comparison where the columns are reversed.  For example, if
    surname is in the forename field and vice versa

    Args:
        col_name_1 (str): First column, e.g. forename
        col_name_2 (str): Second column, e.g. surname
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.
        tf_adjustment_column (str, optional): Column to use for term frequency
            adjustments if an exact match is observed. Defaults to None.

    Returns:
        ComparisonLevel:
    """

    col_1 = InputColumn(col_name_1, sql_dialect=_mutable_params["dialect"])
    col_2 = InputColumn(col_name_2, sql_dialect=_mutable_params["dialect"])

    s = f"{col_1.name_l()} = {col_2.name_r()} and {col_1.name_r()} = {col_2.name_l()}"
    level_dict = {
        "sql_condition": s,
        "label_for_charts": "Exact match on reversed cols",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability

    if tf_adjustment_column:
        level_dict["tf_adjustment_column"] = tf_adjustment_column

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])

from typing import Union

from .input_column import InputColumn
from .comparison_level import ComparisonLevel


# So that we can pass in a sql_dialect to the comparison levels
# that is different depending on whether we're in a SparkComparionLevel,
# DuckDBComparisonLevel etc.
_mutable_params = {
    "dialect": None,
    "levenshtein": "levenshtein",
    "jaro_winkler": "jaro_winkler",
}


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
        ComparisonLevel: A comparison level for a given distance function
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
    col_name,
    m_probability=None,
    term_frequency_adjustments=False,
    include_colname_in_charts_label=False,
) -> ComparisonLevel:

    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])

    label_suffix = f" {col_name}" if include_colname_in_charts_label else ""
    level_dict = {
        "sql_condition": f"{col.name_l()} = {col.name_r()}",
        "label_for_charts": f"Exact match{label_suffix}",
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
        ComparisonLevel: A comparison level that evaluates the levenshtein similarity
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
        ComparisonLevel: A comparison level that evaluates the exact match of two
            columns.
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


def distance_in_km_level(
    lat_col: str,
    long_col: str,
    km_threshold: Union[int, float],
    not_null: bool = False,
    m_probability=None,
) -> ComparisonLevel:
    """Use the haversine formula to transform comparisons of lat,lngs
    into distances measured in kilometers

    Arguments:
        lat_col (str): The name of a latitude column or the respective array
            or struct column column containing the information
            For example: long_lat['lat'] or long_lat[0]
        long_col (str): The name of a longitudinal column or the respective array
            or struct column column containing the information, plus an index.
            For example: long_lat['long'] or long_lat[1]
        km_threshold (int): The total distance in kilometers to evaluate your
            comparisons against
        not_null (bool): If true, remove any . This is only necessary if you are not
            capturing nulls elsewhere in your comparison level.
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.


    Returns:
        ComparisonLevel: A comparison level that evaluates the distance between
            two coordinates
    """

    lat = InputColumn(lat_col, sql_dialect=_mutable_params["dialect"])
    long = InputColumn(long_col, sql_dialect=_mutable_params["dialect"])
    lat_l, lat_r = lat.names_l_r()
    long_l, long_r = long.names_l_r()

    partial_distance_sql = f"""
    (
    pow(sin(radians({lat_r} - {lat_l}))/2, 2) +
    cos(radians({lat_l})) * cos(radians({lat_r})) *
    pow(sin(radians({long_r} - {long_l})/2),2)
    )
    """

    distance_km_sql = f"""
        cast(atan2(sqrt({partial_distance_sql}),
        sqrt(-1*{partial_distance_sql} + 1)) * 12742 as float)
        <= {km_threshold}
    """

    if not_null:
        null_sql = " AND ".join(
            [f"{c} is not null" for c in [lat_r, lat_l, long_l, long_r]]
        )
        distance_km_sql = f"({null_sql}) AND {distance_km_sql}"

    level_dict = {
        "sql_condition": distance_km_sql,
        "label_for_charts": f"Distance less than {km_threshold}km",
    }

    if m_probability:
        level_dict["m_probability"] = m_probability

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])


def percentage_difference_level(
    col_name: str,
    percentage_distance_threshold: float,
    m_probability=None,
):
    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])

    s = f"""(abs({col.name_l()} - {col.name_r()})/
        (case
            when {col.name_r()} > {col.name_l()}
            then {col.name_r()}
            else {col.name_l()}
        end))
        < {percentage_distance_threshold}"""

    level_dict = {
        "sql_condition": s,
        "label_for_charts": f"< {percentage_distance_threshold:,.2%} diff",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])

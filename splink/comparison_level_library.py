from typing import Union

from .input_column import InputColumn
from .comparison_level import ComparisonLevel


# So that we can pass in a sql_dialect to the comparison levels
# tht depending on whether we're in a SparkComparionLevel,
# Du
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


def null_level(col_name, array=False) -> ComparisonLevel:
    """Represents comparisons where one or both sides of the comparison
    contains null values so the similarity cannot be evaluated.

    Assumed to have a partial match weight of zero (null effect on overall match weight)

    Args:
        col_name (str): Input column name
        array (bool): If true, the comparison also checks if the array len is 0.

    Returns:
        ComparisonLevel: Comparison level
    """

    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])

    sql_cond = f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL"
    if array:
        sql_cond += f"\nOR (SIZE({col.name_l()}) = 0 OR SIZE({col.name_r()}) = 0)"

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
        ComparisonLevel: A comparison level that evaluates the exact match of two columns.
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


def compare_multiple_columns_to_single_column_level(
    anchor_column: str,
    col_list: list,
    m_probability=None,
) -> ComparisonLevel:
    """Compare the exact match of multiple columns against a given anchor column. For example,
    surname could be compared against both forename1 and forename2, which would compute whether
    the surname of person 1 matches either forename of person 2. This is useful for comparing
    columns which can be accidentally swapped, such as names and dates (American vs European dates).

    Args:
        anchor_column (str): The column to use as the main comparison column
        col_name_2 (list): A list of columns to compare against the anchor column
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.

    Returns:
        ComparisonLevel: A comparison level that evaluates the exact match of
            multiple columns against a single anchor column.
    """

    anchor_column = InputColumn(anchor_column, sql_dialect=_mutable_params["dialect"])
    comparison_cols = InputColumn(col_list, sql_dialect=_mutable_params["dialect"])

    cc = [
        InputColumn(c, sql_dialect=_mutable_params["dialect"]) for c in comparison_cols
    ]

    sql_cond = " OR ".join([f"{anchor_column.name_l()} = {c.name_r()}" for c in cc])
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": "Exact match on reversed cols",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])


def distance_in_km_level(
    km_threshold: Union[int, float],
    lat_lng_array: str = None,
    lat_col: str = None,
    long_col: str = None,
    not_null: bool = False,
    m_probability=None,
) -> ComparisonLevel:
    """Use the haversine formula to transform comparisons of lat,lngs
    into distances measured in kilometers

    Arguments:
        km_threshold (int): The total distance in kilometers to evaluate your
            comparisons against
        lat_lng_array (str): The column name for a concatenated array containing both
            latitude and longitude in the format: {lat: 53.111111, long: -1.111111}.
        lat_col (str): If your data is not in array form, you can provide the name of the
            latitude column indepedently.
        long_col (str): If your data is not in array form, you can provide the name of the
            longitudinal column indepedently.
        not_null (bool): If true, remove any . This is only necessary if you are not capturing
            nulls elsewhere in your comparison level.
        m_probability (float, optional): Starting value for m probability. Defaults to
            None.


    Returns:
        ComparisonLevel: A comparison level that evaluates the distance between two coordinates
    """

    if lat_lng_array:
        lat_long = InputColumn(lat_lng_array, sql_dialect=_mutable_params["dialect"])
        lat_l, lat_r = f"{lat_long.name_l()}['lat']", f"{lat_long.name_r()}['lat']"
        long_l, long_r = f"{lat_long.name_l()}['long']", f"{lat_long.name_r()}['long']"
    else:
        lat = InputColumn(lat_col, sql_dialect=_mutable_params["dialect"])
        long = InputColumn(long_col, sql_dialect=_mutable_params["dialect"])
        lat_l, lat_r = lat.name_l(), lat.name_r()
        long_l, long_r = long.name_l(), long.name_r()

    partial_distance_sql = f"""
    (
    pow(sin(radians({lat_r} - {lat_l}))/2, 2) +
    cos(radians({lat_l})) * cos(radians({lat_r})) *
    pow(sin(radians({long_r} - {long_l})/2),2)
    )
    """

    distance_km_sql = f"cast(atan2(sqrt({partial_distance_sql}), sqrt(-1*{partial_distance_sql} + 1)) * 12742 as float)"

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

from functools import partial

from .input_column import InputColumn
from .comparison_level import ComparisonLevel


def _pairwise_distance_function_level(
    col_name,
    distance_function_name,
    distance_threshold,
    m_probability=None,
    sql_dialect=None,
) -> ComparisonLevel:
    col = InputColumn(col_name, sql_dialect=sql_dialect)
    sql_cond = (
        f"{distance_function_name}({col.name_l()}, {col.name_r()}) "
        f"<= {distance_threshold}"
    )
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{distance_function_name} <= {distance_threshold}",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability

    return ComparisonLevel(level_dict, sql_dialect=sql_dialect)


def null_level(col_name, sql_dialect=None) -> ComparisonLevel:
    col = InputColumn(col_name, sql_dialect=sql_dialect)
    level_dict = {
        "sql_condition": f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL",
        "label_for_charts": "Null",
        "is_null_level": True,
    }
    return ComparisonLevel(level_dict, sql_dialect=sql_dialect)


def exact_match_level(
    col_name, m_probability=None, term_frequency_adjustments=False, sql_dialect=None
) -> ComparisonLevel:
    col = InputColumn(col_name, sql_dialect=sql_dialect)
    level_dict = {
        "sql_condition": f"{col.name_l()} = {col.name_r()}",
        "label_for_charts": "Exact match",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability
    if term_frequency_adjustments:
        level_dict["tf_adjustment_column"] = col_name

    return ComparisonLevel(level_dict, sql_dialect=sql_dialect)


levenshtein_level = partial(
    _pairwise_distance_function_level, distance_function_name="levenstein"
)


def else_level(
    m_probability=None,
) -> ComparisonLevel:
    level_dict = {
        "sql_condition": "ELSE",
        "label_for_charts": "All other comparisons",
    }
    if m_probability:
        level_dict["m_probability"] = m_probability
    return ComparisonLevel(level_dict)

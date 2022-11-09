from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    levenshtein_level,
    else_level,
    null_level,
    distance_function_level,
    columns_reversed_level,
    jaccard_level,
    jaro_winkler_level,
    distance_in_km_level,
)
from ..input_column import InputColumn
from ..comparison_level import ComparisonLevel

_mutable_params["dialect"] = "spark"


def array_intersect_level(
    col_name, m_probability=None, term_frequency_adjustments=False, min_intersection=1,
    include_colname_in_charts_label=False
) -> ComparisonLevel:

    col = InputColumn(col_name, sql_dialect=_mutable_params["dialect"])

    sql = f"size(array_intersect({col.name_l()}, {col.name_r()})) >= {min_intersection}"

    label_prefix = f"{col_name} arrays" if include_colname_in_charts_label else "Arrays"
    if min_intersection == 1:
        label = f"{label_prefix} intersect"
    else:
        label = f"{label_prefix} intersect size >= {min_intersection}"

    level_dict = {"sql_condition": sql, "label_for_charts": label}
    if m_probability:
        level_dict["m_probability"] = m_probability
    if term_frequency_adjustments:
        level_dict["tf_adjustment_column"] = col_name

    return ComparisonLevel(level_dict, sql_dialect=_mutable_params["dialect"])

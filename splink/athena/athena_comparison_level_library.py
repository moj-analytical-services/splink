from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    levenshtein_level,
    else_level,
    null_level,
    columns_reversed_level,
    distance_in_km_level,
    array_intersect_level,
)

def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"

_mutable_params["dialect"] = "presto"
_mutable_params["levenshtein"] = "levenshtein_distance"
_mutable_params["size_array_intersect_function"] = size_array_intersect_sql

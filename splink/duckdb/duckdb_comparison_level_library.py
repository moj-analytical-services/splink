from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    levenshtein_level,
    jaccard_level,
    jaro_winkler_level,
    else_level,
    null_level,
    distance_function_level,
    columns_reversed_level,
    distance_in_km_level,
    percentage_difference_level,
)

_mutable_params["dialect"] = "duckdb"
_mutable_params["jaro_winkler"] = "jaro_winkler_similarity"

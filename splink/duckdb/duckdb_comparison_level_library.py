from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    levenshtein_level,
    jaccard_level,
    else_level,
    null_level,
    distance_function_level,
    columns_reversed_level,
)

_mutable_params["dialect"] = "duckdb"

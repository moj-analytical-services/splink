from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    levenshtein_at_thresholds,
    distance_function_at_thresholds,
    jaccard_at_thresholds,
    jaro_winkler_at_thresholds,
    array_intersect_at_sizes,
)
from .duckdb_comparison_level_library import (
    size_array_intersect_sql,
)

_mutable_params["jaro_winkler"] = "jaro_winkler_similarity"
_mutable_params["dialect"] = "duckdb"
_mutable_params["size_array_intersect_function"] = size_array_intersect_sql

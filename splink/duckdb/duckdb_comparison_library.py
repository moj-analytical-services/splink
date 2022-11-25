from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    DistanceFunctionAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
    JaroWinklerAtThresholdsComparisonBase,
    JaccardAtThresholdsComparisonBase,
    ArrayIntersectAtSizesComparisonBase,
)
from .duckb_base import (
    DuckDBBase,
)

# _mutable_params["jaro_winkler"] = "jaro_winkler_similarity"
_mutable_params["dialect"] = "duckdb"



class distance_function_at_thresholds(
    DuckDBBase, DistanceFunctionAtThresholdsComparisonBase
):
    pass


class levenshtein_at_thresholds(
    DuckDBBase, LevenshteinAtThresholdsComparisonBase
):
    pass


class jaro_winkler_at_thresholds(
    DuckDBBase, JaroWinklerAtThresholdsComparisonBase
):
    pass


class jaccard_at_thresholds(DuckDBBase, JaccardAtThresholdsComparisonBase):
    pass


class array_intersect_at_sizes(DuckDBBase, ArrayIntersectAtSizesComparisonBase):
    pass

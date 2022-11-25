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
from .duckdb_comparison_level_library import (
    distance_function_level,
    levenshtein_level,
    jaro_winkler_level,
    jaccard_level,
)

# _mutable_params["jaro_winkler"] = "jaro_winkler_similarity"
_mutable_params["dialect"] = "duckdb"



class distance_function_at_thresholds(
    DuckDBBase, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    DuckDBBase, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class jaro_winkler_at_thresholds(
    DuckDBBase, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return jaro_winkler_level


class jaccard_at_thresholds(DuckDBBase, JaccardAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return jaccard_level


class array_intersect_at_sizes(DuckDBBase, ArrayIntersectAtSizesComparisonBase):
    pass

from ..comparison_library import (
    ExactMatchBase,
    DistanceFunctionAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
    JaroWinklerAtThresholdsComparisonBase,
    JaccardAtThresholdsComparisonBase,
    ArrayIntersectAtSizesComparisonBase,
    DateDiffAtThresholdsComparisonBase,
)
from .duckdb_base import (
    DuckDBBase,
)
from .duckdb_comparison_level_library import (
    exact_match_level,
    null_level,
    else_level,
    distance_function_level,
    levenshtein_level,
    jaro_winkler_level,
    jaccard_level,
    array_intersect_level,
    datediff_level,
)


class DuckDBComparisonProperties(DuckDBBase):
    @property
    def _exact_match_level(self):
        return exact_match_level

    @property
    def _null_level(self):
        return null_level

    @property
    def _else_level(self):
        return else_level

    @property
    def _datediff_level(self):
        return datediff_level

    @property
    def _array_intersect_level(self):
        return array_intersect_level


class exact_match(DuckDBComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    DuckDBComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    DuckDBComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class jaro_winkler_at_thresholds(
    DuckDBComparisonProperties, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return jaro_winkler_level


class jaccard_at_thresholds(
    DuckDBComparisonProperties, JaccardAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return jaccard_level


class array_intersect_at_sizes(
    DuckDBComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class datediff_at_thresholds(
    DuckDBComparisonProperties, DateDiffAtThresholdsComparisonBase
):
    pass

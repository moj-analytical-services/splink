from ..comparison_library import (
    ArrayIntersectAtSizesComparisonBase,
    DateDiffAtThresholdsComparisonBase,
    DistanceFunctionAtThresholdsComparisonBase,
    DistanceInKMAtThresholdsComparisonBase,
    ExactMatchBase,
    JaccardAtThresholdsComparisonBase,
    JaroAtThresholdsComparisonBase,
    JaroWinklerAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
)
from .duckdb_comparison_level_library import DuckDBComparisonProperties


class exact_match(DuckDBComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    DuckDBComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    DuckDBComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class jaro_at_thresholds(DuckDBComparisonProperties, JaroAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return self._jaro_level


class jaro_winkler_at_thresholds(
    DuckDBComparisonProperties, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaro_winkler_level


class jaccard_at_thresholds(
    DuckDBComparisonProperties, JaccardAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaccard_level


class array_intersect_at_sizes(
    DuckDBComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class datediff_at_thresholds(
    DuckDBComparisonProperties, DateDiffAtThresholdsComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    DuckDBComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass

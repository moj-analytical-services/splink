from ..comparison_library import (
    ArrayIntersectAtSizesComparisonBase,
    DistanceFunctionAtThresholdsComparisonBase,
    DistanceInKMAtThresholdsComparisonBase,
    ExactMatchBase,
    LevenshteinAtThresholdsComparisonBase,
)
from .athena_comparison_level_library import AthenaComparisonProperties


class exact_match(AthenaComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    AthenaComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    AthenaComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class array_intersect_at_sizes(
    AthenaComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    AthenaComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass

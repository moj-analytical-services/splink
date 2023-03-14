from ..comparison_library import (
    ArrayIntersectAtSizesComparisonBase,
    DistanceFunctionAtThresholdsComparisonBase,
    DistanceInKMAtThresholdsComparisonBase,
    ExactMatchBase,
    LevenshteinAtThresholdsComparisonBase,
)
from .athena_base import (
    AthenaBase,
)
from .athena_comparison_level_library import (
    array_intersect_level,
    distance_function_level,
    distance_in_km_level,
    else_level,
    exact_match_level,
    levenshtein_level,
    null_level,
)


class AthenaComparisonProperties(AthenaBase):
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
    def _array_intersect_level(self):
        return array_intersect_level

    @property
    def _distance_in_km_level(self):
        return distance_in_km_level


class exact_match(AthenaComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    AthenaComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    AthenaComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class array_intersect_at_sizes(
    AthenaComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    AthenaComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass

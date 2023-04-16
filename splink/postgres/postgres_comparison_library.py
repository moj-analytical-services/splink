from ..comparison_library import (
    ArrayIntersectAtSizesComparisonBase,
    DateDiffAtThresholdsComparisonBase,
    DistanceFunctionAtThresholdsComparisonBase,
    DistanceInKMAtThresholdsComparisonBase,
    ExactMatchBase,
    JaccardAtThresholdsComparisonBase,
    JaroWinklerAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
)
from .postgres_base import PostgresBase
from .postgres_comparison_level_library import (
    array_intersect_level,
    datediff_level,
    distance_function_level,
    distance_in_km_level,
    else_level,
    exact_match_level,
    jaccard_level,
    jaro_winkler_level,
    levenshtein_level,
    null_level,
)


class PostgresComparisonProperties(PostgresBase):
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

    @property
    def _distance_in_km_level(self):
        return distance_in_km_level

    @property
    def _levenshtein_level(self):
        return levenshtein_level

    @property
    def _jaro_winkler_level(self):
        return jaro_winkler_level

    @property
    def _jaccard_level(self):
        return jaccard_level


class exact_match(PostgresComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    PostgresComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    PostgresComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class jaro_winkler_at_thresholds(
    PostgresComparisonProperties, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return jaro_winkler_level


class jaccard_at_thresholds(
    PostgresComparisonProperties, JaccardAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return jaccard_level


class array_intersect_at_sizes(
    PostgresComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class datediff_at_thresholds(
    PostgresComparisonProperties, DateDiffAtThresholdsComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    PostgresComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass

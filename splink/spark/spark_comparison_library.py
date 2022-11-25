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

from .spark_base import (
    SparkBase,
)
from .spark_comparison_level_library import (
    distance_function_level,
    levenshtein_level,
    jaro_winkler_level,
    jaccard_level,
    array_intersect_level,
)


_mutable_params["dialect"] = "spark"


class distance_function_at_thresholds(
    SparkBase, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(SparkBase, LevenshteinAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return levenshtein_level


class jaro_winkler_at_thresholds(SparkBase, JaroWinklerAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return jaro_winkler_level


class jaccard_at_thresholds(SparkBase, JaccardAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return jaccard_level


class array_intersect_at_sizes(SparkBase, ArrayIntersectAtSizesComparisonBase):
    @property
    def _array_intersect_level(self):
        return array_intersect_level

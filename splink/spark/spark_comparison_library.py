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


_mutable_params["dialect"] = "spark"


class distance_function_at_thresholds(
    SparkBase, DistanceFunctionAtThresholdsComparisonBase
):
    pass


class levenshtein_at_thresholds(
    SparkBase, LevenshteinAtThresholdsComparisonBase
):
    pass


class jaro_winkler_at_thresholds(
    SparkBase, JaroWinklerAtThresholdsComparisonBase
):
    pass


class jaccard_at_thresholds(SparkBase, JaccardAtThresholdsComparisonBase):
    pass


class array_intersect_at_sizes(SparkBase, ArrayIntersectAtSizesComparisonBase):
    pass

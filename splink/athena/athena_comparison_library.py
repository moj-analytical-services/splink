from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    DistanceFunctionAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
    ArrayIntersectAtSizesComparisonBase,
)

from .athena_base import (
    AthenaBase,
)
from .athena_comparison_level_library import (
    distance_function_level,
    levenshtein_level,
)

_mutable_params["dialect"] = "presto"


class distance_function_at_thresholds(
    AthenaBase, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    AthenaBase, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class array_intersect_at_sizes(AthenaBase, ArrayIntersectAtSizesComparisonBase):
    pass

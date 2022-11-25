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

_mutable_params["dialect"] = "presto"


class distance_function_at_thresholds(
    AthenaBase, DistanceFunctionAtThresholdsComparisonBase
):
    pass


class levenshtein_at_thresholds(
    AthenaBase, LevenshteinAtThresholdsComparisonBase
):
    pass


class array_intersect_at_sizes(AthenaBase, ArrayIntersectAtSizesComparisonBase):
    pass

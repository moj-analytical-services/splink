from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    levenshtein_at_thresholds,
    distance_function_at_thresholds,
    jaccard_at_thresholds,
    jaro_winkler_at_thresholds,
    ArrayIntersectAtSizesComparisonBase,
)

from .spark_comparison_level_library import (
    array_intersect_level,
)


_mutable_params["dialect"] = "spark"


class array_intersect_at_sizes(ArrayIntersectAtSizesComparisonBase):
    @property
    def _array_intersect_level(self):
        return array_intersect_level

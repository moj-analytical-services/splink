from ..comparison_level_library import (
    _mutable_params,
)


from ..comparison_library import (  # noqa: F401
    exact_match,
    levenshtein_at_thresholds,
    ArrayIntersectAtSizesComparisonBase,
)

from .athena_comparison_level_library import (
    array_intersect_level,
)

_mutable_params["dialect"] = "presto"
_mutable_params["levenshtein"] = "levenshtein_distance"


class array_intersect_at_sizes(ArrayIntersectAtSizesComparisonBase):
    @property
    def _array_intersect_level(self):
        return array_intersect_level

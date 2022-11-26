from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    LevenshteinLevelBase,
    DistanceFunctionLevelBase,
    ElseLevelBase,
    null_level,
    ColumnsReversedLevelBase,
    DistanceInKMLevelBase,
    ArrayIntersectLevelBase,
)
from .athena_base import (
    AthenaBase,
)

_mutable_params["dialect"] = "presto"


class else_level(AthenaBase, ElseLevelBase):
    pass


class columns_reversed_level(AthenaBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(AthenaBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(AthenaBase, LevenshteinLevelBase):
    pass


class array_intersect_level(AthenaBase, ArrayIntersectLevelBase):
    pass


class distance_in_km_level(AthenaBase, DistanceInKMLevelBase):
    pass

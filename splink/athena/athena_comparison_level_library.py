from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    LevenshteinLevelBase,
    DistanceFunctionLevelBase,
    else_level,
    null_level,
    columns_reversed_level,
    distance_in_km_level,
    ArrayIntersectLevelBase,
)
from .athena_base import (
    AthenaBase,
)

_mutable_params["dialect"] = "presto"


class distance_function_level(AthenaBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(AthenaBase, LevenshteinLevelBase):
    pass


class array_intersect_level(ArrayIntersectLevelBase):
    pass

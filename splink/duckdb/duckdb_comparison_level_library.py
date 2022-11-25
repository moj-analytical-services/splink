from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    LevenshteinLevelBase,
    JaccardLevelBase,
    JaroWinklerLevelBase,
    else_level,
    null_level,
    DistanceFunctionLevelBase,
    columns_reversed_level,
    DistanceInKMLevelBase,
    PercentageDifferenceLevelBase,
    ArrayIntersectLevelBase,
)
from .duckb_base import (
    DuckDBBase,
)

_mutable_params["dialect"] = "duckdb"


class distance_function_level(DuckDBBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(DuckDBBase, LevenshteinLevelBase):
    pass


class jaro_winkler_level(DuckDBBase, JaroWinklerLevelBase):
    pass


class jaccard_level(DuckDBBase, JaccardLevelBase):
    pass


class array_intersect_level(DuckDBBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(DuckDBBase, PercentageDifferenceLevelBase):
    pass


class distance_in_km_level(DuckDBBase, DistanceInKMLevelBase):
    pass

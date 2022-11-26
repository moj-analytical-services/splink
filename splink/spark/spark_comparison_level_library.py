from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    LevenshteinLevelBase,
    ElseLevelBase,
    null_level,
    DistanceFunctionLevelBase,
    ColumnsReversedLevelBase,
    JaccardLevelBase,
    JaroWinklerLevelBase,
    DistanceInKMLevelBase,
    ArrayIntersectLevelBase,
)
from .spark_base import (
    SparkBase,
)

_mutable_params["dialect"] = "spark"


class else_level(SparkBase, ElseLevelBase):
    pass


class columns_reversed_level(SparkBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(SparkBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(SparkBase, LevenshteinLevelBase):
    pass


class jaro_winkler_level(SparkBase, JaroWinklerLevelBase):
    pass


class jaccard_level(SparkBase, JaccardLevelBase):
    pass


class array_intersect_level(SparkBase, ArrayIntersectLevelBase):
    pass


class distance_in_km_level(SparkBase, DistanceInKMLevelBase):
    pass

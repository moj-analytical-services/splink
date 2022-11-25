from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    LevenshteinLevelBase,
    else_level,
    null_level,
    DistanceFunctionLevelBase,
    columns_reversed_level,
    JaccardLevelBase,
    JaroWinklerLevelBase,
    distance_in_km_level,
    ArrayIntersectLevelBase,
)
from .spark_base import (
    SparkBase,
)

_mutable_params["dialect"] = "spark"


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

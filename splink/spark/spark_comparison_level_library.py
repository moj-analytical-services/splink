from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    DialectLevel,
    exact_match_level,
    LevenshteinLevelBase,
    else_level,
    null_level,
    DistanceFunctionLevelBase,
    columns_reversed_level,
    jaccard_level,
    JaroWinklerLevelBase,
    distance_in_km_level,
    ArrayIntersectLevelBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"size(array_intersect({col_name_l}, {col_name_r}))"


_mutable_params["dialect"] = "spark"



class SparkLevel(DialectLevel):

    @property
    def _sql_dialect(self):
        return "spark"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql


class distance_function_level(SparkLevel, DistanceFunctionLevelBase):
    pass

class levenshtein_level(SparkLevel, LevenshteinLevelBase):
    pass

class jaro_winkler_level(SparkLevel, JaroWinklerLevelBase):
    pass

class array_intersect_level(SparkLevel, ArrayIntersectLevelBase):
    pass

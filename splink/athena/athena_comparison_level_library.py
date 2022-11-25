from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    DialectLevel,
    exact_match_level,
    LevenshteinLevelBase,
    levenshtein_level,
    DistanceFunctionLevelBase,
    else_level,
    null_level,
    columns_reversed_level,
    distance_in_km_level,
    ArrayIntersectLevelBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"


_mutable_params["dialect"] = "presto"


class AthenaLevel(DialectLevel):
    @property
    def _sql_dialect(self):
        return "presto"

    @property
    def _levenshtein_name(self):
        return "levenshtein_distance"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

class distance_function_level(AthenaLevel, DistanceFunctionLevelBase):
    pass

class levenshtein_level(AthenaLevel, LevenshteinLevelBase):
    pass

class array_intersect_level(ArrayIntersectLevelBase):
    pass

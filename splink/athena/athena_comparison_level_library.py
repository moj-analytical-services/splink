from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    DialectLevel,
    exact_match_level,
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
_mutable_params["levenshtein"] = "levenshtein_distance"


class AthenaLevel(DialectLevel):
    @property
    def _sql_dialect(self):
        return "presto"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

class distance_function_level(AthenaLevel, DistanceFunctionLevelBase):
    pass


class array_intersect_level(ArrayIntersectLevelBase):
    pass

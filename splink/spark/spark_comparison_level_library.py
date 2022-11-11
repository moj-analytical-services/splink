from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    levenshtein_level,
    else_level,
    null_level,
    distance_function_level,
    columns_reversed_level,
    jaccard_level,
    jaro_winkler_level,
    distance_in_km_level,
    ArrayIntersectLevelBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"size(array_intersect({col_name_l}, {col_name_r}))"


_mutable_params["dialect"] = "spark"


class array_intersect_level(ArrayIntersectLevelBase):
    @property
    def _sql_dialect_(self):
        return "spark"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

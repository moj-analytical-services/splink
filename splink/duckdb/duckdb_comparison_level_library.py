from ..comparison_level_library import (
    ExactMatchLevelBase,
    LevenshteinLevelBase,
    JaccardLevelBase,
    JaroWinklerLevelBase,
    ElseLevelBase,
    NullLevelBase,
    DistanceFunctionLevelBase,
    ColumnsReversedLevelBase,
    DistanceInKMLevelBase,
    PercentageDifferenceLevelBase,
    ArrayIntersectLevelBase,
)
from .duckb_base import (
    DuckDBBase,
)


class null_level(DuckDBBase, NullLevelBase):
    pass


class exact_match_level(DuckDBBase, ExactMatchLevelBase):
    pass


class else_level(DuckDBBase, ElseLevelBase):
    pass


class columns_reversed_level(DuckDBBase, ColumnsReversedLevelBase):
    pass


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

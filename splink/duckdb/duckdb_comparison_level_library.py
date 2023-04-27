from ..comparison_level_composition import and_, not_, or_  # noqa: F401
from ..comparison_level_library import (
    ArrayIntersectLevelBase,
    ColumnsReversedLevelBase,
    DateDiffLevelBase,
    DistanceFunctionLevelBase,
    DistanceInKMLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    JaccardLevelBase,
    JaroLevelBase,
    JaroWinklerLevelBase,
    LevenshteinLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from .duckdb_base import (
    DuckDBBase,
)


# Class used to feed our comparison_library classes
class DuckDBComparisonProperties(DuckDBBase):
    @property
    def _exact_match_level(self):
        return exact_match_level

    @property
    def _null_level(self):
        return null_level

    @property
    def _else_level(self):
        return else_level

    @property
    def _datediff_level(self):
        return datediff_level

    @property
    def _array_intersect_level(self):
        return array_intersect_level

    @property
    def _distance_in_km_level(self):
        return distance_in_km_level

    @property
    def _distance_function_level(self):
        return distance_function_level

    @property
    def _levenshtein_level(self):
        return levenshtein_level

    @property
    def _jaro_level(self):
        return jaro_level

    @property
    def _jaro_winkler_level(self):
        return jaro_winkler_level

    @property
    def _jaccard_level(self):
        return jaccard_level


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


class jaro_level(DuckDBBase, JaroLevelBase):
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


class datediff_level(DuckDBBase, DateDiffLevelBase):
    pass

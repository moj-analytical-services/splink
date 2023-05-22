from ...comparison_level_library import (
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
from ...comparison_library import (
    ArrayIntersectAtSizesComparisonBase,
    DamerauLevenshteinAtThresholdsComparisonBase,
    DateDiffAtThresholdsComparisonBase,
    DistanceFunctionAtThresholdsComparisonBase,
    DistanceInKMAtThresholdsComparisonBase,
    ExactMatchBase,
    JaccardAtThresholdsComparisonBase,
    JaroAtThresholdsComparisonBase,
    JaroWinklerAtThresholdsComparisonBase,
    LevenshteinAtThresholdsComparisonBase,
)
from ...comparison_template_library import (
    DateComparisonBase,
    ForenameSurnameComparisonBase,
    NameComparisonBase,
    PostcodeComparisonBase,
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
    def _columns_reversed_level(self):
        return columns_reversed_level

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
    def _damerau_levenshtein_level(self):
        return damerau_levenshtein_level

    @property
    def _jaro_level(self):
        return jaro_level

    @property
    def _jaro_winkler_level(self):
        return jaro_winkler_level

    @property
    def _jaccard_level(self):
        return jaccard_level


#########################
### COMPARISON LEVELS ###
#########################
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


class damerau_levenshtein_level(DuckDBBase, LevenshteinLevelBase):
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


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(DuckDBComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    DuckDBComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    DuckDBComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class damerau_levenshtein_at_thresholds(
    DuckDBComparisonProperties, DamerauLevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._damerau_levenshtein_level


class jaro_at_thresholds(DuckDBComparisonProperties, JaroAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return self._jaro_level


class jaro_winkler_at_thresholds(
    DuckDBComparisonProperties, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaro_winkler_level


class jaccard_at_thresholds(
    DuckDBComparisonProperties, JaccardAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaccard_level


class array_intersect_at_sizes(
    DuckDBComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class datediff_at_thresholds(
    DuckDBComparisonProperties, DateDiffAtThresholdsComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    DuckDBComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
class date_comparison(DuckDBComparisonProperties, DateComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class name_comparison(DuckDBComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class forename_surname_comparison(
    DuckDBComparisonProperties, ForenameSurnameComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class postcode_comparison(DuckDBComparisonProperties, PostcodeComparisonBase):
    pass

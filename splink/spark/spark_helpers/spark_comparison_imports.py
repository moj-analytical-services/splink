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
from .spark_base import (
    SparkBase,
)


# Class used to feed our comparison_library classes
class SparkComparisonProperties(SparkBase):
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
class null_level(SparkBase, NullLevelBase):
    pass


class exact_match_level(SparkBase, ExactMatchLevelBase):
    pass


class else_level(SparkBase, ElseLevelBase):
    pass


class columns_reversed_level(SparkBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(SparkBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(SparkBase, LevenshteinLevelBase):
    pass


class damerau_levenshtein_level(SparkBase, LevenshteinLevelBase):
    pass


class jaro_level(SparkBase, JaroLevelBase):
    pass


class jaro_winkler_level(SparkBase, JaroWinklerLevelBase):
    pass


class jaccard_level(SparkBase, JaccardLevelBase):
    pass


class array_intersect_level(SparkBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(SparkBase, PercentageDifferenceLevelBase):
    pass


class distance_in_km_level(SparkBase, DistanceInKMLevelBase):
    pass


class datediff_level(SparkBase, DateDiffLevelBase):
    pass


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(SparkComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SparkComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    SparkComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class damerau_levenshtein_at_thresholds(
    SparkComparisonProperties, DamerauLevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._damerau_levenshtein_level


class jaro_at_thresholds(SparkComparisonProperties, JaroAtThresholdsComparisonBase):
    @property
    def _distance_level(self):
        return self._jaro_level


class jaro_winkler_at_thresholds(
    SparkComparisonProperties, JaroWinklerAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaro_winkler_level


class jaccard_at_thresholds(
    SparkComparisonProperties, JaccardAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._jaccard_level


class array_intersect_at_sizes(
    SparkComparisonProperties, ArrayIntersectAtSizesComparisonBase
):
    pass


class datediff_at_thresholds(
    SparkComparisonProperties, DateDiffAtThresholdsComparisonBase
):
    pass


class distance_in_km_at_thresholds(
    SparkComparisonProperties, DistanceInKMAtThresholdsComparisonBase
):
    pass


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
class date_comparison(SparkComparisonProperties, DateComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class name_comparison(SparkComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class forename_surname_comparison(
    SparkComparisonProperties, ForenameSurnameComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class postcode_comparison(SparkComparisonProperties, PostcodeComparisonBase):
    pass

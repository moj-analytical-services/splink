from ...comparison_level_library import (
    ArrayIntersectLevelBase,
    ColumnsReversedLevelBase,
    DatediffLevelBase,
    DistanceFunctionLevelBase,
    DistanceInKMLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    LevenshteinLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from ...comparison_library import (
    ArrayIntersectAtSizesBase,
    DatediffAtThresholdsBase,
    DistanceFunctionAtThresholdsBase,
    DistanceInKMAtThresholdsBase,
    ExactMatchBase,
    LevenshteinAtThresholdsBase,
)
from ...comparison_template_library import (
    PostcodeComparisonBase,
)
from .athena_base import (
    AthenaBase,
)


# Class used to feed our comparison_library classes
class AthenaComparisonProperties(AthenaBase):
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
    def _array_intersect_level(self):
        return array_intersect_level

    @property
    def _columns_reversed_level(self):
        return columns_reversed_level

    @property
    def _datediff_level(self):
        return datediff_level

    @property
    def _distance_in_km_level(self):
        return distance_in_km_level

    @property
    def _distance_function_level(self):
        return distance_function_level

    @property
    def _levenshtein_level(self):
        return levenshtein_level


#########################
### COMPARISON LEVELS ###
#########################
class null_level(AthenaBase, NullLevelBase):
    pass


class exact_match_level(AthenaBase, ExactMatchLevelBase):
    pass


class else_level(AthenaBase, ElseLevelBase):
    pass


class columns_reversed_level(AthenaBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(AthenaBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(AthenaBase, LevenshteinLevelBase):
    pass


class array_intersect_level(AthenaBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(AthenaBase, PercentageDifferenceLevelBase):
    pass


class datediff_level(AthenaBase, DatediffLevelBase):
    pass


class distance_in_km_level(AthenaBase, DistanceInKMLevelBase):
    pass


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(AthenaComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    AthenaComparisonProperties, DistanceFunctionAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    AthenaComparisonProperties, LevenshteinAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class array_intersect_at_sizes(AthenaComparisonProperties, ArrayIntersectAtSizesBase):
    pass


class datediff_at_thresholds(AthenaComparisonProperties, DatediffAtThresholdsBase):
    pass


class distance_in_km_at_thresholds(
    AthenaComparisonProperties, DistanceInKMAtThresholdsBase
):
    pass


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
class postcode_comparison(AthenaComparisonProperties, PostcodeComparisonBase):
    pass

from ...comparison_level_library import (
    ArrayIntersectLevelBase,
    ColumnsReversedLevelBase,
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
    DistanceFunctionAtThresholdsBase,
    DistanceInKMAtThresholdsBase,
    ExactMatchBase,
    LevenshteinAtThresholdsBase,
)
from .redshift_base import (
    RedshiftBase,
)


# Class used to feed our comparison_library classes
class RedshiftComparisonProperties(RedshiftBase):
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
class null_level(RedshiftBase, NullLevelBase):
    pass


class exact_match_level(RedshiftBase, ExactMatchLevelBase):
    pass


class else_level(RedshiftBase, ElseLevelBase):
    pass


class columns_reversed_level(RedshiftBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(RedshiftBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(RedshiftBase, LevenshteinLevelBase):
    pass


class array_intersect_level(RedshiftBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(RedshiftBase, PercentageDifferenceLevelBase):
    pass


class distance_in_km_level(RedshiftBase, DistanceInKMLevelBase):
    pass


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(RedshiftComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    RedshiftComparisonProperties, DistanceFunctionAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._distance_function_level


class levenshtein_at_thresholds(
    RedshiftComparisonProperties, LevenshteinAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class array_intersect_at_sizes(RedshiftComparisonProperties, ArrayIntersectAtSizesBase):
    pass


class distance_in_km_at_thresholds(
    RedshiftComparisonProperties, DistanceInKMAtThresholdsBase
):
    pass


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
# Not yet implemented
# Currently does not support the necessary comparison levels
# required for existing comparison templates

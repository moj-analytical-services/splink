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
from .postgres_base import (
    PostgresBase,
)


# Class used to feed our comparison_library classes
class PostgresComparisonProperties(PostgresBase):
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
    def _levenshtein_level(self):
        return levenshtein_level


#########################
### COMPARISON LEVELS ###
#########################
class null_level(PostgresBase, NullLevelBase):
    pass


class exact_match_level(PostgresBase, ExactMatchLevelBase):
    pass


class else_level(PostgresBase, ElseLevelBase):
    pass


class columns_reversed_level(PostgresBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(PostgresBase, DistanceFunctionLevelBase):
    pass


class levenshtein_level(PostgresBase, LevenshteinLevelBase):
    pass


class array_intersect_level(PostgresBase, ArrayIntersectLevelBase):
    pass


class percentage_difference_level(PostgresBase, PercentageDifferenceLevelBase):
    pass


class distance_in_km_level(PostgresBase, DistanceInKMLevelBase):
    pass


class datediff_level(PostgresBase, DatediffLevelBase):
    pass


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(PostgresComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    PostgresComparisonProperties, DistanceFunctionAtThresholdsBase
):
    @property
    def _distance_level(self):
        return distance_function_level


class levenshtein_at_thresholds(
    PostgresComparisonProperties, LevenshteinAtThresholdsBase
):
    @property
    def _distance_level(self):
        return levenshtein_level


class array_intersect_at_sizes(PostgresComparisonProperties, ArrayIntersectAtSizesBase):
    pass


class datediff_at_thresholds(PostgresComparisonProperties, DatediffAtThresholdsBase):
    pass


class distance_in_km_at_thresholds(
    PostgresComparisonProperties, DistanceInKMAtThresholdsBase
):
    pass


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
# Not yet implemented
# Currently does not support the necessary comparison levels
# required for existing comparison templates

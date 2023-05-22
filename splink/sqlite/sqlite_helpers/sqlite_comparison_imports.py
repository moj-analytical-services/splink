from ...comparison_level_library import (
    ColumnsReversedLevelBase,
    DistanceFunctionLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from ...comparison_library import (
    DistanceFunctionAtThresholdsComparisonBase,
    ExactMatchBase,
)
from .sqlite_base import (
    SqliteBase,
)


# Class used to feed our comparison_library classes
class SqliteComparisonProperties(SqliteBase):
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
    def _distance_function_level(self):
        return distance_function_level

    @property
    def _columns_reversed_level(self):
        return columns_reversed_level


#########################
### COMPARISON LEVELS ###
#########################
class null_level(SqliteBase, NullLevelBase):
    pass


class exact_match_level(SqliteBase, ExactMatchLevelBase):
    pass


class else_level(SqliteBase, ElseLevelBase):
    pass


class columns_reversed_level(SqliteBase, ColumnsReversedLevelBase):
    pass


class distance_function_level(SqliteBase, DistanceFunctionLevelBase):
    pass


class percentage_difference_level(SqliteBase, PercentageDifferenceLevelBase):
    pass


##########################
### COMPARISON LIBRARY ###
##########################
class exact_match(SqliteComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SqliteComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    pass

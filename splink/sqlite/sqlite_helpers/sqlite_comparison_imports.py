from ...comparison_level_library import (
    ColumnsReversedLevelBase,
    DamerauLevenshteinLevelBase,
    DistanceFunctionLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    JaroLevelBase,
    JaroWinklerLevelBase,
    LevenshteinLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from ...comparison_library import (
    DamerauLevenshteinAtThresholdsBase,
    DistanceFunctionAtThresholdsBase,
    ExactMatchBase,
    JaroAtThresholdsBase,
    JaroWinklerAtThresholdsBase,
    LevenshteinAtThresholdsBase,
)
from ...comparison_template_library import (
    ForenameSurnameComparisonBase,
    NameComparisonBase,
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


class levenshtein_level(SqliteBase, LevenshteinLevelBase):
    pass


class damerau_levenshtein_level(SqliteBase, DamerauLevenshteinLevelBase):
    pass


class jaro_level(SqliteBase, JaroLevelBase):
    pass


class jaro_winkler_level(SqliteBase, JaroWinklerLevelBase):
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
    SqliteComparisonProperties, DistanceFunctionAtThresholdsBase
):
    pass


class levenshtein_at_thresholds(
    SqliteComparisonProperties, LevenshteinAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level


class damerau_levenshtein_at_thresholds(
    SqliteComparisonProperties, DamerauLevenshteinAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._damerau_levenshtein_level


class jaro_at_thresholds(SqliteComparisonProperties, JaroAtThresholdsBase):
    @property
    def _distance_level(self):
        return self._jaro_level


class jaro_winkler_at_thresholds(
    SqliteComparisonProperties, JaroWinklerAtThresholdsBase
):
    @property
    def _distance_level(self):
        return self._jaro_winkler_level


###################################
### COMPARISON TEMPLATE LIBRARY ###
###################################
class name_comparison(SqliteComparisonProperties, NameComparisonBase):
    @property
    def _distance_level(self):
        return distance_function_level


class forename_surname_comparison(
    SqliteComparisonProperties, ForenameSurnameComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level

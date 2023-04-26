from ..comparison_library import (
    DistanceFunctionAtThresholdsComparisonBase,
    ExactMatchBase,
    LevenshteinAtThresholdsComparisonBase,
)
from .sqlite_comparison_level_library import SqliteComparisonProperties


class exact_match(SqliteComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SqliteComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    pass


class levenshtein_at_thresholds(
    SqliteComparisonProperties, LevenshteinAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return self._levenshtein_level

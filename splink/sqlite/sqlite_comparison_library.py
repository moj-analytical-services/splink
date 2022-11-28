from ..comparison_library import (
    ExactMatchBase,
    DistanceFunctionAtThresholdsComparisonBase,
)
from .sqlite_base import (
    SqliteBase,
)
from .sqlite_comparison_level_library import (
    exact_match_level,
    null_level,
    else_level,
    distance_function_level,
)


class SqliteComparison(SqliteBase):
    @property
    def _exact_match_level(self):
        return exact_match_level

    @property
    def _null_level(self):
        return null_level

    @property
    def _else_level(self):
        return else_level


class exact_match(SqliteComparison, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SqliteComparison, DistanceFunctionAtThresholdsComparisonBase
):
    @property
    def _distance_level(self):
        return distance_function_level

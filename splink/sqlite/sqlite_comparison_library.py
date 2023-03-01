from ..comparison_library import (
    DistanceFunctionAtThresholdsComparisonBase,
    ExactMatchBase,
)
from .sqlite_base import (
    SqliteBase,
)
from .sqlite_comparison_level_library import (
    distance_function_level,
    else_level,
    exact_match_level,
    null_level,
)


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
    def _distance_level(self):
        return distance_function_level


class exact_match(SqliteComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SqliteComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    pass

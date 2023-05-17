from ..comparison_library import (
    DistanceFunctionAtThresholdsComparisonBase,
    ExactMatchBase,
)
from .sqlite_comparison_level_library import SqliteComparisonProperties


class exact_match(SqliteComparisonProperties, ExactMatchBase):
    pass


class distance_function_at_thresholds(
    SqliteComparisonProperties, DistanceFunctionAtThresholdsComparisonBase
):
    pass

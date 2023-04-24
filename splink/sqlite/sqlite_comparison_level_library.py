from ..comparison_level_composition import and_, not_, or_  # noqa: F401
from ..comparison_level_library import (
    ColumnsReversedLevelBase,
    DistanceFunctionLevelBase,
    ElseLevelBase,
    ExactMatchLevelBase,
    NullLevelBase,
    PercentageDifferenceLevelBase,
)
from .sqlite_base import (
    SqliteBase,
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
    def _distance_function_level(self):
        return distance_function_level


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

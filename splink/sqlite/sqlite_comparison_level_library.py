from ..comparison_level_library import (
    NullLevelBase,
    ExactMatchLevelBase,
    ElseLevelBase,
    ColumnsReversedLevelBase,
    DistanceFunctionLevelBase,
    PercentageDifferenceLevelBase,
)
from .sqlite_base import (
    SqliteBase,
)


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

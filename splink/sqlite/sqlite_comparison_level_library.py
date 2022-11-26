from ..comparison_level_library import (
    NullLevelBase,
    ExactMatchLevelBase,
    ElseLevelBase,
    DistanceFunctionLevelBase,
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


class distance_function_level(SqliteBase, DistanceFunctionLevelBase):
    pass

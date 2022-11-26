from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    NullLevelBase,
    ExactMatchLevelBase,
    ElseLevelBase,
    DistanceFunctionLevelBase,
)
from .sqlite_base import (
    SqliteBase,
)

_mutable_params["dialect"] = "sqlite"


class null_level(SqliteBase, NullLevelBase):
    pass


class exact_match_level(SqliteBase, ExactMatchLevelBase):
    pass


class else_level(SqliteBase, ElseLevelBase):
    pass


class distance_function_level(SqliteBase, DistanceFunctionLevelBase):
    pass

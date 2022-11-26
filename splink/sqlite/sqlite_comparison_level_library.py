from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    exact_match_level,
    ElseLevelBase,
    null_level,
    DistanceFunctionLevelBase,
)
from .sqlite_base import (
    SqliteBase,
)

_mutable_params["dialect"] = "sqlite"


class else_level(SqliteBase, ElseLevelBase):
    pass


class distance_function_level(SqliteBase, DistanceFunctionLevelBase):
    pass

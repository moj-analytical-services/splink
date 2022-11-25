from ..comparison_level_library import (  # noqa: F401
    _mutable_params,
    DialectLevel,
    exact_match_level,
    else_level,
    null_level,
    DistanceFunctionLevelBase,
)

_mutable_params["dialect"] = "sqlite"


class SqliteLevel(DialectLevel):
    @property
    def _sql_dialect(self):
        return "sqlite"


class distance_function_level(SqliteLevel, DistanceFunctionLevelBase):
    pass

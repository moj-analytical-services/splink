from __future__ import annotations

from typing import Iterable, Union, final

from .blocking import BlockingRule
from .comparison_creator import ComparisonLevelCreator
from .comparison_level import ComparisonLevel
from .comparison_level_library import CustomLevel
from .dialects import SplinkDialect


def _ensure_is_comparison_level_creator(
    cl: Union[ComparisonLevelCreator, dict]
) -> ComparisonLevelCreator:
    if isinstance(cl, dict):
        # TODO: proper dict => level method
        return CustomLevel(**dict)
    if isinstance(cl, ComparisonLevelCreator):
        return cl
    raise TypeError(
        f"parameter 'cl' must be a `ComparisonLevelCreator` or `dict`, "
        f"but is of type {type(cl)} [{cl}]"
    )


class _Merge(ComparisonLevelCreator):
    @final
    def __init__(self, *comparison_levels: Union[ComparisonLevelCreator, dict]):
        num_levels = len(comparison_levels)
        if num_levels == 0:
            raise ValueError(f"Must provide at least one level to {type(self)}()")
        self.comparison_levels = [
            _ensure_is_comparison_level_creator(cl)
            for cl in comparison_levels
        ]
        self.is_null_level = all(cl.is_null_level for cl in comparison_levels)

    @final
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.comparison_levels)
        )

    @final
    def create_label_for_charts(self) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_label_for_charts()})", self.comparison_levels)
        )


class And(_Merge):
    _clause = "AND"


class Or(_Merge):
    _clause = "OR"


class Not(ComparisonLevelCreator):
    def __init__(self, comparison_level: Union[ComparisonLevelCreator, dict]):
        self.comparison_level = _ensure_is_comparison_level_creator(comparison_level)
        # turn null levels into non-null levels, otherwise do nothing
        if self.comparison_level.is_null_level:
            self.is_null_level = False

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f"NOT ({self.comparison_level.create_sql(sql_dialect)})"

    def create_label_for_charts(self) -> str:
        return f"NOT ({self.comparison_level.create_label_for_charts()})"


def _unify_sql_dialects(cls: Iterable[ComparisonLevel | BlockingRule]) -> str | None:
    sql_dialects = set(cl.sql_dialect for cl in cls)
    sql_dialects.discard(None)
    if len(sql_dialects) > 1:
        raise ValueError("Cannot combine comparison levels with different SQL dialects")
    elif len(sql_dialects) == 0:
        return None
    return sql_dialects.pop()

from __future__ import annotations

from typing import Iterable, Union

from .blocking import BlockingRule
from .comparison_creator import ComparisonLevelCreator
from .comparison_level import ComparisonLevel
from .dialects import SplinkDialect


class And(ComparisonLevelCreator):
    def __init__(self, *comparison_levels: Union[ComparisonLevelCreator, dict]):
        num_levels = len(comparison_levels)
        if num_levels == 0:
            raise ValueError("Must provide at least one level to And()")
        self.comparison_levels = comparison_levels
        self.is_null_level = all(cl.is_null_level for cl in comparison_levels)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return " AND ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.comparison_levels)
        )

    def create_label_for_charts(self) -> str:
        return " AND ".join(
            map(lambda cl: f"({cl.create_label_for_charts()})", self.comparison_levels)
        )


class Or(ComparisonLevelCreator):
    def __init__(self, *comparison_levels: Union[ComparisonLevelCreator, dict]):
        num_levels = len(comparison_levels)
        if num_levels == 0:
            raise ValueError("Must provide at least one level to Or()")
        self.comparison_levels = comparison_levels
        self.is_null_level = all(cl.is_null_level for cl in comparison_levels)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return " OR ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.comparison_levels)
        )

    def create_label_for_charts(self) -> str:
        return " OR ".join(
            map(lambda cl: f"({cl.create_label_for_charts()})", self.comparison_levels)
        )


class Not(ComparisonLevelCreator):
    def __init__(self, comparison_level: Union[ComparisonLevelCreator, dict]):
        self.comparison_level = comparison_level
        # turn null levels into non-null levels, otherwise do nothing
        if comparison_level.is_null_level:
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

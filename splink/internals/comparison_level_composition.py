from __future__ import annotations

from typing import Any, Iterable, Union, final

from splink.internals.blocking import BlockingRule
from splink.internals.dialects import SplinkDialect

from .comparison_creator import ComparisonLevelCreator
from .comparison_level import ComparisonLevel


def _ensure_is_comparison_level_creator(
    cl: Union[ComparisonLevelCreator, dict[str, Any]],
) -> ComparisonLevelCreator:
    if isinstance(cl, dict):
        from .comparison_level_library import CustomLevel

        return CustomLevel(**cl)
    if isinstance(cl, ComparisonLevelCreator):
        return cl
    raise TypeError(
        f"parameter 'cl' must be a `ComparisonLevelCreator` or `dict`, "
        f"but is of type {type(cl)} [{cl}]"
    )


class _Merge(ComparisonLevelCreator):
    _clause: str = ""

    @final
    def __init__(
        self, *comparison_levels: Union[ComparisonLevelCreator, dict[str, Any]]
    ):
        num_levels = len(comparison_levels)
        if num_levels == 0:
            raise ValueError(f"Must provide at least one level to {type(self)}()")
        self.comparison_levels = [
            _ensure_is_comparison_level_creator(cl) for cl in comparison_levels
        ]
        self.is_null_level = all(cl.is_null_level for cl in self.comparison_levels)

    @final
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.comparison_levels)
        )

    def create_label_for_charts(self) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_label_for_charts()})", self.comparison_levels)
        )


class And(_Merge):
    """
    Represents a comparison level that is an 'AND' of other comparison levels

    Merge multiple ComparisonLevelCreators into a single ComparisonLevelCreator by
    merging their SQL conditions using a logical "AND".

    Args:
        *comparison_levels (ComparisonLevelCreator | dict): These represent the
            comparison levels you wish to combine via 'AND'
    """

    _clause = "AND"


class Or(_Merge):
    """
    Represents a comparison level that is an 'OR' of other comparison levels

    Merge multiple ComparisonLevelCreators into a single ComparisonLevelCreator by
    merging their SQL conditions using a logical "OR".

    Args:
        *comparison_levels (ComparisonLevelCreator | dict): These represent the
            comparison levels you wish to combine via 'OR'
    """

    _clause = "OR"


class Not(ComparisonLevelCreator):
    """
    Represents a comparison level that is the negation of another comparison level

    Resulting ComparisonLevelCreator is equivalent to the passed ComparisonLevelCreator
    but with SQL conditions negated with logical "NOY".

    Args:
        *comparison_level (ComparisonLevelCreator | dict): This represents the
            comparison level you wish to negate with 'NOT'
    """

    def __init__(self, comparison_level: Union[ComparisonLevelCreator, dict[str, Any]]):
        self.comparison_level = _ensure_is_comparison_level_creator(comparison_level)
        # turn null levels into non-null levels, otherwise do nothing
        if self.comparison_level.is_null_level:
            self.is_null_level = False

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f"NOT ({self.comparison_level.create_sql(sql_dialect)})"

    def create_label_for_charts(self) -> str:
        return f"NOT ({self.comparison_level.create_label_for_charts()})"


def _unify_sql_dialects(cls: Iterable[ComparisonLevel | BlockingRule]) -> str | None:
    sql_dialects = set(cl.sqlglot_dialect for cl in cls)
    sql_dialects.discard(None)
    if len(sql_dialects) > 1:
        raise ValueError("Cannot combine comparison levels with different SQL dialects")
    elif len(sql_dialects) == 0:
        return None
    return sql_dialects.pop()

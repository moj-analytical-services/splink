from abc import ABC, abstractmethod
from typing import Union, final

from .blocking import BlockingRule
from .column_expression import ColumnExpression
from .dialects import SplinkDialect


class BlockingRuleCreator(ABC):
    def __init__(self, salting_partitions=None, arrays_to_explode=None):
        self._salting_partitions = salting_partitions
        self._arrays_to_explode = arrays_to_explode

    # @property because merged levels need logic to determine salting partitions
    @property
    def salting_partitions(self):
        return self._salting_partitions

    @property
    def arrays_to_explode(self):
        return self._arrays_to_explode

    @abstractmethod
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        pass

    @final
    def create_blocking_rule_dict(self, sql_dialect_str: str) -> dict:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict = {
            "blocking_rule": self.create_sql(sql_dialect),
        }

        if self.salting_partitions and self.arrays_to_explode:
            raise ValueError("Cannot use both salting_partitions and arrays_to_explode")

        if self.salting_partitions:
            level_dict["salting_partitions"] = self.salting_partitions

        if self.arrays_to_explode:
            level_dict["arrays_to_explode"] = self.arrays_to_explode

        return level_dict

    @final
    def get_blocking_rule(self, sql_dialect_str: str) -> BlockingRule:
        return BlockingRule(self.create_blocking_rule_dict(sql_dialect_str))


class ExactMatchRule(BlockingRuleCreator):
    def __init__(
        self,
        col_name_or_expr: Union[str, ColumnExpression],
        salting_partitions=None,
        arrays_to_explode=None,
    ):
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        self.col_expression = ColumnExpression.instantiate_if_str(col_name_or_expr)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"{col.l_name} = {col.r_name}"


class _Merge(BlockingRuleCreator):
    @final
    def __init__(self, *blocking_rules: Union[BlockingRuleCreator, dict]):
        num_levels = len(blocking_rules)
        if num_levels == 0:
            raise ValueError(
                f"Must provide at least one blocking rule to {type(self)}()"
            )
        self.blocking_rules = blocking_rules

    @property
    def salting_partitions(self):
        if hasattr(self, "_salting_partitions"):
            return self._salting_partitions

        return max(
            [
                br.salting_partitions
                for br in self.blocking_rules
                if br.salting_partitions is not None
            ],
            default=None,
        )

    @property
    def arrays_to_explode(self):
        if hasattr(self, "_arrays_to_explode"):
            return self._arrays_to_explode

        if any([br.arrays_to_explode for br in self.blocking_rules]):
            raise ValueError("Cannot merge blocking rules with arrays_to_explode")
        return None

    @final
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.blocking_rules)
        )


class And(_Merge):
    _clause = "AND"


# Should we bother with merge and And or just have block on?
def block_on(
    *col_names_or_exprs: Union[str, ColumnExpression],
    salting_partitions=None,
    arrays_to_explode=None,
) -> BlockingRuleCreator:
    br = And(*[ExactMatchRule(c) for c in col_names_or_exprs])
    if salting_partitions:
        br._salting_partitions = salting_partitions
    if arrays_to_explode:
        br._arrays_to_explode = arrays_to_explode
    return br

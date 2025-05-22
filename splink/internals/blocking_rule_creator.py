from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Union, final

from splink.internals.blocking import (
    BlockingRule,
    BlockingRuleDict,
    blocking_rule_to_obj,
)
from splink.internals.dialects import SplinkDialect


class BlockingRuleCreator(ABC):
    def __init__(
        self,
        salting_partitions: int | None = None,
        arrays_to_explode: list[str] | None = None,
    ):
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
    def create_blocking_rule_dict(self, sql_dialect_str: str) -> BlockingRuleDict:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict: BlockingRuleDict = {
            "blocking_rule": self.create_sql(sql_dialect),
            "sql_dialect": sql_dialect_str,
            "salting_partitions": self.salting_partitions,
            "arrays_to_explode": self.arrays_to_explode,
        }

        if self.salting_partitions and self.arrays_to_explode:
            raise ValueError("Cannot use both salting_partitions and arrays_to_explode")

        return level_dict

    @final
    def get_blocking_rule(self, sql_dialect_str: str) -> BlockingRule:
        return blocking_rule_to_obj(self.create_blocking_rule_dict(sql_dialect_str))


acceptable_br_creator_types = Union[BlockingRuleCreator, str, Dict[str, Any]]

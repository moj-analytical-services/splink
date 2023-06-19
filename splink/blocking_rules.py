from __future__ import annotations

from .blocking import BlockingRule
from .input_column import InputColumn


class exact_match_rule(BlockingRule):
    def __init__(
        self,
        col_name: str,
        salting_partitions: int = 1,
    ) -> BlockingRule:

        col = InputColumn(col_name, sql_dialect=self.sql_dialect)
        col = col.name()
        blocking_rule = f"l.{col} = r.{col}"
        self._description = "Exact match"

        super().__init__(
            blocking_rule,
            salting_partitions=salting_partitions,
        )
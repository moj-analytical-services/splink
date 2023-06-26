from __future__ import annotations

from .blocking import BlockingRule
from .input_column import InputColumn


class exact_match_rule(BlockingRule):
    def __init__(
        self,
        col_name: str,
        salting_partitions: int = 1,
    ) -> BlockingRule:
        """Represents an exact match blocking rule.

        Args:
            col_name (str): Input column name
            salting_partitions (optional, int): Whether to add salting
                to the blocking rule. More information on salting can
                be found within the docs. Salting is only valid for Spark.
        Examples:
            === ":simple-duckdb: DuckDB"
                Simple Exact match level
                ``` python
                import splink.duckdb.blocking_rule_library as brl
                brl.exact_match_rule("name")
                ```
            === ":simple-apachespark: Spark"
                Simple Exact match level
                ``` python
                import splink.spark.blocking_rule_library as brl
                brl.exact_match_rule("name", salting_partitions=1)
                ```
            === ":simple-amazonaws: Athena"
                Simple Exact match level
                ``` python
                import splink.athena.blocking_rule_library as brl
                brl.exact_match_rule("name")
                ```
            === ":simple-sqlite: SQLite"
                Simple Exact match level
                ``` python
                import splink.sqlite.blocking_rule_library as brl
                brl.exact_match_rule("name")
                ```
            === "PostgreSQL"
                Simple Exact match level
                ``` python
                import splink.postgres.blocking_rule_library as brl
                brl.exact_match_rule("name")
                ```
        """

        col = InputColumn(col_name, sql_dialect=self.sql_dialect)
        col = col.name()
        blocking_rule = f"l.{col} = r.{col}"
        self._description = "Exact match"

        super().__init__(
            blocking_rule,
            salting_partitions=salting_partitions,
        )

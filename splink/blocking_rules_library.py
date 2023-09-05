from __future__ import annotations

import warnings

import sqlglot

from .blocking import BlockingRule
from .blocking_rule_composition import and_
from .misc import ensure_is_list
from .sql_transform import add_quotes_and_table_prefix


class exact_match_rule(BlockingRule):
    def __init__(
        self,
        col_name: str,
        salting_partitions: int = 1,
    ) -> BlockingRule:
        """Represents an exact match blocking rule.

        DEPRECATED:
        `exact_match_rule` is deprecated. Please use `block_on`
        instead, which acts as a wrapper with additional functionality.

        Args:
            col_name (str): Input column name, or a str represent a sql
                statement you'd like to match on. For example, `surname` or
                `"substr(surname,1,2)"` are both valid.
            salting_partitions (optional, int): Whether to add salting
                to the blocking rule. More information on salting can
                be found within the docs. Salting is currently only valid
                for Spark.
        Examples:
            === ":simple-duckdb: DuckDB"
                Simple Exact match level
                ``` python
                import splink.duckdb.blocking_rule_library as brl
                brl.exact_match_rule("name")

                sql = "substr(surname,1,2)"
                brl.exact_match_rule(sql)
                ```
            === ":simple-apachespark: Spark"
                Simple Exact match level
                ``` python
                import splink.spark.blocking_rule_library as brl
                brl.exact_match_rule("name", salting_partitions=1)

                sql = "substr(surname,1,2)"
                brl.exact_match_rule(sql)
                ```
            === ":simple-amazonaws: Athena"
                Simple Exact match level
                ``` python
                import splink.athena.blocking_rule_library as brl
                brl.exact_match_rule("name")

                sql = "substr(surname,1,2)"
                brl.exact_match_rule(sql)
                ```
            === ":simple-sqlite: SQLite"
                Simple Exact match level
                ``` python
                import splink.sqlite.blocking_rule_library as brl
                brl.exact_match_rule("name")

                sql = "substr(surname,1,2)"
                brl.exact_match_rule(sql)
                ```
            === "PostgreSQL"
                Simple Exact match level
                ``` python
                import splink.postgres.blocking_rule_library as brl
                brl.exact_match_rule("name")

                sql = "substr(surname,1,2)"
                brl.exact_match_rule(sql)
                ```
        """

        warnings.warn(
            "`exact_match_rule` is deprecated; use `block_on`",
            DeprecationWarning,
            stacklevel=2,
        )

        syntax_tree = sqlglot.parse_one(col_name, read=self._sql_dialect)

        l_col = add_quotes_and_table_prefix(syntax_tree, "l").sql(self._sql_dialect)
        r_col = add_quotes_and_table_prefix(syntax_tree, "r").sql(self._sql_dialect)

        blocking_rule = f"{l_col} = {r_col}"
        self._description = "Exact match"

        super().__init__(
            blocking_rule,
            salting_partitions=salting_partitions,
        )


def block_on(
    _exact_match,
    col_names: list[str],
    salting_partitions: int = 1,
) -> BlockingRule:
    """Creates a series of blocking rules on designated columns or sql
        statements, that are then joined by "AND" clauses.

    It is recommended that you try to ensure your rules are equi-joins
    and only include `AND` clauses to join rules.

    More information can be found
    [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html)

    This function acts as a shorthand alias for the `brl.and_` syntax:
    > import splink.duckdb.blocking_rule_library as brl
    > `brl.and_(brl.exact_match_rule, brl.exact_match_rule, ...)`

        Args:
            col_names (list[str]): A list of input columns or sql conditions
                you wish to create blocks on.
            salting_partitions (optional, int): Whether to add salting
                to the blocking rule. More information on salting can
                be found within the docs. Salting is only valid for Spark.
        Examples:
            === ":simple-duckdb: DuckDB"
                ``` python
                import splink.duckdb.blocking_rule_library as brl
                sql = "substr(surname,1,2)"
                block_on([sql, "surname"])
                ```
            === ":simple-apachespark: Spark"
                ``` python
                import splink.spark.blocking_rule_library as brl
                sql = "substr(surname,1,2)"
                block_on([sql, "surname"], salting_partitions=1)
                ```
            === ":simple-amazonaws: Athena"
                ``` python
                import splink.athena.blocking_rule_library as brl
                sql = "substr(surname,1,2)"
                block_on([sql, "surname"])
                ```
            === ":simple-sqlite: SQLite"
                ``` python
                import splink.sqlite.blocking_rule_library as brl
                sql = "substr(surname,1,2)"
                block_on([sql, "surname"])
                ```
            === "PostgreSQL"
                ``` python
                import splink.postgres.blocking_rule_library as brl
                sql = "substr(surname,1,2)"
                block_on([sql, "surname"])
                ```
    """

    col_names = ensure_is_list(col_names)
    em_rules = [_exact_match(col) for col in col_names]
    return and_(*em_rules, salting_partitions=salting_partitions)

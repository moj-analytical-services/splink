from __future__ import annotations

import warnings

import sqlglot

from .blocking import BlockingRule, blocking_rule_to_obj
from .blocking_rule_composition import and_
from .misc import ensure_is_list
from .sql_transform import add_quotes_and_table_prefix


def exact_match_rule(
    col_name: str,
    _sql_dialect: str,
    salting_partitions: int = None,
) -> BlockingRule:
    """Represents an exact match blocking rule.

    **DEPRECATED:**
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
    """
    warnings.warn(
        "`exact_match_rule` is deprecated; use `block_on`",
        DeprecationWarning,
        stacklevel=2,
    )

    syntax_tree = sqlglot.parse_one(col_name, read=_sql_dialect)

    l_col = add_quotes_and_table_prefix(syntax_tree, "l").sql(_sql_dialect)
    r_col = add_quotes_and_table_prefix(syntax_tree, "r").sql(_sql_dialect)

    blocking_rule = f"{l_col} = {r_col}"

    return blocking_rule_to_obj(
        {
            "blocking_rule": blocking_rule,
            "salting_partitions": salting_partitions,
            "sql_dialect": _sql_dialect,
        }
    )


def block_on(
    _exact_match,
    col_names: list[str],
    salting_partitions: int = 1,
) -> BlockingRule:
    """The `block_on` function generates blocking rules that facilitate
    efficient equi-joins based on the columns or SQL statements
    specified in the col_names argument. When multiple columns or
    SQL snippets are provided, the function generates a compound
    blocking rule, connecting individual match conditions with
    "AND" clauses.

    This function is designed for scenarios where you aim to achieve
    efficient yet straightforward blocking conditions based on one
    or more columns or SQL snippets.

    For more information on the intended use cases of `block_on`, please see
    [the following discussion](https://github.com/moj-analytical-services/splink/issues/1376).

    Further information on equi-join conditions can be found
    [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html)

    This function acts as a shorthand alias for the `brl.and_` syntax:
    ```py
    import splink.duckdb.blocking_rule_library as brl
    brl.and_(brl.exact_match_rule, brl.exact_match_rule, ...)
    ```

    Args:
        col_names (list[str]): A list of input columns or sql conditions
            you wish to create blocks on.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. More information on salting can
            be found within the docs. Salting is only valid for Spark.

    Examples:
        === ":simple-duckdb: DuckDB"
            ``` python
            from splink.duckdb.blocking_rule_library import block_on
            block_on("first_name")  # check for exact matches on first name
            sql = "substr(surname,1,2)"
            block_on([sql, "surname"])
            ```
        === ":simple-apachespark: Spark"
            ``` python
            from splink.spark.blocking_rule_library import block_on
            block_on("first_name")  # check for exact matches on first name
            sql = "substr(surname,1,2)"
            block_on([sql, "surname"], salting_partitions=1)
            ```
        === ":simple-amazonaws: Athena"
            ``` python
            from splink.athena.blocking_rule_library import block_on
            block_on("first_name")  # check for exact matches on first name
            sql = "substr(surname,1,2)"
            block_on([sql, "surname"])
            ```
        === ":simple-sqlite: SQLite"
            ``` python
            from splink.sqlite.blocking_rule_library import block_on
            block_on("first_name")  # check for exact matches on first name
            sql = "substr(surname,1,2)"
            block_on([sql, "surname"])
            ```
        === "PostgreSQL"
            ``` python
            from splink.postgres.blocking_rule_library import block_on
            block_on("first_name")  # check for exact matches on first name
            sql = "substr(surname,1,2)"
            block_on([sql, "surname"])
            ```
    """  # noqa: E501

    col_names = ensure_is_list(col_names)
    em_rules = [_exact_match(col) for col in col_names]
    return and_(*em_rules, salting_partitions=salting_partitions)

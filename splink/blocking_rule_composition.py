from __future__ import annotations

from .blocking import BlockingRule
from .composition_helpers import _and_, _not_, _or_


def and_(
    *brls: BlockingRule | str, salting_partitions: int = 1, **kwargs
) -> BlockingRule:
    """Merge BlockingRules using logical "AND".

    Merge multiple BlockingRules into a single BlockingRule by
    merging their SQL conditions using a logical "AND".


    Args:
        *brls (BlockingRule | str): BlockingRules or
            blocking rules as strings.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. More information on salting can
            be found within the docs. Salting is only valid for Spark.

    Examples:
        === "DuckDB"
            Simple exact rule composition with an `AND` clause
            ``` python
            import splink.duckdb.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                brl.exact_match_rule("surname")
            )
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.duckdb.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)"
            )
            ```
        === "Spark"
            Simple exact rule composition with an `AND` clause
            ``` python
            import splink.spark.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                brl.exact_match_rule("surname")
            )
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column, with additional salting (spark exclusive)
            ``` python
            import splink.spark.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
                salting_partitions=5
            )
            ```
        === "Athena"
            Simple exact rule composition with an `AND` clause
            ``` python
            import splink.athena.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                brl.exact_match_rule("surname")
            )
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.athena.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
        === "SQLite"
            Simple exact rule composition with an `AND` clause
            ``` python
            import splink.sqlite.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                brl.exact_match_rule("surname")
            )
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.sqlite.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
        === "PostgreSQL"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.postgres.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                brl.exact_match_rule("surname")
            )
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.postgres.blocking_rule_library as brl
            brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    return _and_(*brls, salting_partitions=salting_partitions, **kwargs)


def or_(
    *brls: BlockingRule | str, salting_partitions: int = 1, **kwargs
) -> BlockingRule:
    """Merge BlockingRules using logical "OR".

    Merge multiple BlockingRules into a single BlockingRule by
    merging their SQL conditions using a logical "OR".


    Args:
        *brls (BlockingRule | str): BlockingRules or
            blocking rules as strings.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. More information on salting can
            be found within the docs. Salting is only valid for Spark.

    Examples:
        === "DuckDB"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.duckdb.blocking_rule_library as brl
            brl.or_(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname"))
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.duckdb.blocking_rule_library as brl
            brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)"
            )
            ```
        === "Spark"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.spark.blocking_rule_library as brl
            brl.or_(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname"))
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column, with additional salting (spark exclusive)
            ``` python
            import splink.spark.blocking_rule_library as brl
            brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
                salting_partitions=5
            )
            ```
        === "Athena"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.athena.blocking_rule_library as brl
            brl.or_(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname"))
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.athena.blocking_rule_library as brl
            brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
        === "SQLite"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.sqlite.blocking_rule_library as brl
            brl.or_(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname"))
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.sqlite.blocking_rule_library as brl
            brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
        === "PostgreSQL"
            Simple exact rule composition with an `OR` clause
            ``` python
            import splink.postgres.blocking_rule_library as brl
            brl.or_(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname"))
            ```
            Composing a custom rule with an exact match on name and the year
            from a date of birth column
            ``` python
            import splink.postgres.blocking_rule_library as brl
            brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    return _or_(*brls, salting_partitions=salting_partitions, **kwargs)


def not_(
    brl: BlockingRule | str, salting_partitions: int = 1, **kwargs
) -> BlockingRule:
    """Invert a BlockingRule using "NOT".

    Returns a BlockingRule with the same SQL condition as the input,
    but prefixed with "NOT".

    Args:
        *brls (BlockingRule | str): BlockingRules or
            blocking rules as strings.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. More information on salting can
            be found within the docs. Salting is only valid for Spark.

    Examples:
        === "DuckDB"
            Block where we do *not* have an exact match on first name
            ``` python
            import splink.duckdb.blocking_rule_library as brl
            brl.not_(brl.exact_match_rule("first_name"))
            ```
        === "Spark"
            Block where we do *not* have an exact match on first name
            ``` python
            import splink.spark.blocking_rule_library as brl
            brl.not_(brl.exact_match_rule("first_name"))
            ```
        === "Athena"
            Block where we do *not* have an exact match on first name
            ``` python
            import splink.athena.blocking_rule_library as brl
            brl.not_(brl.exact_match_rule("first_name"))
            ```
        === "SQLite"
            Block where we do *not* have an exact match on first name
            ``` python
            import splink.sqlite.blocking_rule_library as brl
            brl.not_(brl.exact_match_rule("first_name"))
            ```
        === "PostgreSQL"
            Block where we do *not* have an exact match on first name
            ``` python
            import splink.postgres.blocking_rule_library as brl
            brl.not_(brl.exact_match_rule("first_name"))
            ```

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    return _not_(
        brl, class_name="BlockingRule", salting_partitions=salting_partitions, **kwargs
    )

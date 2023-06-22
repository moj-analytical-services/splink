from __future__ import annotations

import warnings

from .blocking import BlockingRule, blocking_rule_to_obj
from .comparison_level_composition import _unify_sql_dialects


def and_(
    *brls: BlockingRule | dict | str,
    salting_partitions=1,
) -> BlockingRule:
    """Merge BlockingRules using logical "AND".

    Merge multiple BlockingRules into a single BlockingRule by
    merging their SQL conditions using a logical "AND".


    Args:
        *brls (BlockingRule | dict | str): BlockingRules or
            blocking rules in the string/dictionary format.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. Please see the docs for more
            information on salting. Salting is only valid for Spark.

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
            rule = brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)"
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l."first_name" = r."first_name") ' \
            >  'AND (substr(l.dob,1,4) = substr(r.dob,1,4))',
            >}
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
            rule = brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
                salting_partitions=5
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'AND (substr(l.dob,1,4) = substr(r.dob,1,4))'
            > 'salting_partitions': 5
            >}
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
            rule = brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'AND (substr(l.dob,1,4) = substr(r.dob,1,4))'
            >}
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
            rule = brl.and_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'AND (substr(l.dob,1,4) = substr(r.dob,1,4))'
            >}

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    return _br_merge(
        *brls,
        clause="AND",
        salting_partitions=salting_partitions,
    )


def or_(
    *brls: BlockingRule | dict | str,
    salting_partitions: int = 1,
) -> BlockingRule:
    """Merge BlockingRules using logical "OR".

    Merge multiple BlockingRules into a single BlockingRule by
    merging their SQL conditions using a logical "OR".


    Args:
        *brls (BlockingRule | dict | str): BlockingRules or
            blocking rules in the string/dictionary format.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. Please see the docs for more
            information on salting. Salting is only valid for Spark.

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
            rule = brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)"
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l."first_name" = r."first_name") ' \
            >  'OR (substr(l.dob,1,4) = substr(r.dob,1,4))',
            >}
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
            rule = brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
                salting_partitions=5
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'OR (substr(l.dob,1,4) = substr(r.dob,1,4))'
            > 'salting_partitions': 5
            >}
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
            rule = brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'OR (substr(l.dob,1,4) = substr(r.dob,1,4))'
            >}
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
            rule = brl.or_(
                brl.exact_match_rule("first_name"),
                "substr(l.dob,1,4) = substr(r.dob,1,4)",
            )
            ```
            ```python
            rule.as_dict()
            ```
            >{
            > 'sql_condition': '(l.`first_name` = r.`first_name`) ' \
            >  'OR (substr(l.dob,1,4) = substr(r.dob,1,4))'
            >}

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    return _br_merge(
        *brls,
        clause="OR",
        salting_partitions=salting_partitions,
    )


def not_(*brls: BlockingRule | dict | str, salting_partitions: int = 1) -> BlockingRule:
    """Invert a BlockingRule using "NOT".

    Returns a BlockingRule with the same SQL condition as the input,
    but prefixed with "NOT".

    Args:
        *brls (BlockingRule | dict | str): BlockingRules or
            blocking rules in the string/dictionary format.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. Please see the docs for more
            information on salting. Salting is only valid for Spark.

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

    Returns:
        BlockingRule: A new BlockingRule with the merged
            SQL condition
    """
    if len(brls) == 0:
        raise ValueError("You must provide at least one BlockingRule")
    elif len(brls) > 1:
        warnings.warning(
            "More than one BlockingRule entered for `NOT` composition. "
            "This function only accepts one argument and will only use your "
            "first BlockingRule.",
            SyntaxWarning,
            stacklevel=2,
        )

    brls, sql_dialect, salt = _parse_blocking_rules(*brls)
    br = brls[0]
    blocking_rule = f"NOT ({br.blocking_rule})"

    return BlockingRule(
        blocking_rule,
        salting_partitions=salting_partitions if salting_partitions > 1 else salt,
        sql_dialect=sql_dialect,
    )


def _br_merge(
    *brls: BlockingRule | dict | str,
    clause: str,
    salting_partitions: int = 1,
) -> BlockingRule:
    if len(brls) == 0:
        raise ValueError("You must provide at least one BlockingRule")

    brs, sql_dialect, salt = _parse_blocking_rules(*brls)
    conditions = (f"({br.blocking_rule})" for br in brs)
    blocking_rule = f" {clause} ".join(conditions)

    return BlockingRule(
        blocking_rule,
        salting_partitions=salting_partitions if salting_partitions > 1 else salt,
        sql_dialect=sql_dialect,
    )


def _parse_blocking_rules(
    *brs: BlockingRule | dict | str,
) -> tuple[list[BlockingRule], str | None]:
    brs = [_to_blocking_rule(br) for br in brs]
    sql_dialect = _unify_sql_dialects(brs)
    salting_partitions = max([br.salting_partitions for br in brs])
    return brs, sql_dialect, salting_partitions


def _to_blocking_rule(br):
    return blocking_rule_to_obj(br)

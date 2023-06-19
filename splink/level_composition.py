from __future__ import annotations

import warnings
from typing import Iterable

from .blocking import BlockingRule, blocking_rule_to_obj
from .comparison_level import ComparisonLevel


def cl_and_(
    *clls: ComparisonLevel | dict,
    label_for_charts=None,
    m_probability=None,
    is_null_level=None,
) -> ComparisonLevel:
    """Merge ComparisonLevels using logical "AND".

    Merge multiple ComparisonLevels into a single ComparisonLevel by
    merging their SQL conditions using a logical "AND".

    By default, we generate a new `label_for_charts` for the new ComparisonLevel.
    You can override this, and any other ComparisonLevel attributes, by passing
    them as keyword arguments.

    Args:
        *clls (ComparisonLevel | dict): ComparisonLevels or comparison
            level dictionaries to merge
        label_for_charts (str, optional): A label for this comparson level,
            which will appear on charts as a reminder of what the level represents.
            Defaults to a composition of - `label_1 AND label_2`
        m_probability (float, optional): Starting value for m probability.
            Defaults to None.
        is_null_level (bool, optional): If true, m and u values will not be
            estimated and instead the match weight will be zero for this column.
            Defaults to None.

    Examples:
        === "DuckDB"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.duckdb.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.duckdb.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Spark"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.spark.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.spark.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Athena"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.athena.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.athena.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "SQLite"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.sqlite.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```

    Returns:
        ComparisonLevel: A new ComparisonLevel with the merged
            SQL condition
    """
    return _cl_merge(
        *clls,
        clause="AND",
        label_for_charts=label_for_charts,
        m_probability=m_probability,
        is_null_level=is_null_level,
    )


def cl_or_(
    *clls: ComparisonLevel | dict,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
    is_null_level: bool | None = None,
) -> ComparisonLevel:
    """Merge ComparisonLevels using logical "OR".

    Merge multiple ComparisonLevels into a single ComparisonLevel by
    merging their SQL conditions using a logical "OR".

    By default, we generate a new `label_for_charts` for the new ComparisonLevel.
    You can override this, and any other ComparisonLevel attributes, by passing
    them as keyword arguments.

    Args:
        *clls (ComparisonLevel | dict): ComparisonLevels or comparison
            level dictionaries to merge
        label_for_charts (str, optional): A label for this comparson level,
            which will appear on charts as a reminder of what the level represents.
            Defaults to a composition of - `label_1 OR label_2`
        m_probability (float, optional): Starting value for m probability.
            Defaults to None.
        is_null_level (bool, optional): If true, m and u values will not be
            estimated and instead the match weight will be zero for this column.
            Defaults to None.

    Examples:
        === "DuckDB"
            Simple null level composition with an `OR` clause
            ``` python
            import splink.duckdb.comparison_level_library as cll
            cll.or_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.duckdb.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.or_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'OR ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Spark"
            Simple null level composition with an `OR` clause
            ``` python
            import splink.spark.comparison_level_library as cll
            cll.or_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.spark.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.or_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'OR ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Athena"
            Simple null level composition with an `OR` clause
            ``` python
            import splink.athena.comparison_level_library as cll
            cll.or_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.athena.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.or_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'OR ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "SQLite"
            Simple null level composition with an `OR` clause
            ``` python
            import splink.sqlite.comparison_level_library as cll
            cll.or_(cll.null_level("first_name"), cll.null_level("surname"))
            ```

    Returns:
        ComparisonLevel: A new ComparisonLevel with the merged
            SQL condition
    """

    return _cl_merge(
        *clls,
        clause="OR",
        label_for_charts=label_for_charts,
        m_probability=m_probability,
        is_null_level=is_null_level,
    )


def cl_not_(
    cll: ComparisonLevel | dict,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
) -> ComparisonLevel:
    """Negate a ComparisonLevel.

    Returns a ComparisonLevel with the same SQL condition as the input,
    but prefixed with "NOT".

    By default, we generate a new `label_for_charts` for the new ComparisonLevel.
    You can override this, and any other ComparisonLevel attributes, by passing
    them as keyword arguments.

    Args:
        cll (ComparisonLevel | dict): ComparisonLevel or comparison
            level dictionary
        label_for_charts (str, optional): A label for this comparson level,
            which will appear on charts as a reminder of what the level represents.
        m_probability (float, optional): Starting value for m probability.
            Defaults to None.

    Examples:
        === "DuckDB"
            *Not* an exact match on first name
            ``` python
            import splink.duckdb.comparison_level_library as cll
            cll.not_(cll.exact_match("first_name"))
            ```
            Find all exact matches *not* on the first of January
            ``` python
            import splink.duckdb.comparison_level_library as cll
            dob_first_jan =  {
               "sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'",
               "label_for_charts": "Date is 1st Jan",
            }
            exact_match_not_first_jan = cll.and_(
                cll.exact_match_level("dob"),
                cll.not_(dob_first_jan),
                label_for_charts = "Exact match and not the 1st Jan"
            )
            ```
        === "Spark"
            *Not* an exact match on first name
            ``` python
            import splink.spark.comparison_level_library as cll
            cll.not_(cll.exact_match("first_name"))
            ```
            Find all exact matches *not* on the first of January
            ``` python
            import splink.spark.comparison_level_library as cll
            dob_first_jan =  {
               "sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'",
               "label_for_charts": "Date is 1st Jan",
            }
            exact_match_not_first_jan = cll.and_(
                cll.exact_match_level("dob"),
                cll.not_(dob_first_jan),
                label_for_charts = "Exact match and not the 1st Jan"
            )
            ```
        === "Athena"
            *Not* an exact match on first name
            ``` python
            import splink.athena.comparison_level_library as cll
            cll.not_(cll.exact_match("first_name"))
            ```
            Find all exact matches *not* on the first of January
            ``` python
            import splink.athena.comparison_level_library as cll
            dob_first_jan =  {
               "sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'",
               "label_for_charts": "Date is 1st Jan",
            }
            exact_match_not_first_jan = cll.and_(
                cll.exact_match_level("dob"),
                cll.not_(dob_first_jan),
                label_for_charts = "Exact match and not the 1st Jan"
            )
            ```
        === "SQLite"
            *Not* an exact match on first name
            ``` python
            import splink.sqlite.comparison_level_library as cll
            cll.not_(cll.exact_match("first_name"))
            ```
            Find all exact matches *not* on the first of January
            ``` python
            import splink.sqlite.comparison_level_library as cll
            dob_first_jan =  {
               "sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'",
               "label_for_charts": "Date is 1st Jan",
            }
            exact_match_not_first_jan = cll.and_(
                cll.exact_match_level("dob"),
                cll.not_(dob_first_jan),
                label_for_charts = "Exact match and not the 1st Jan"
            )
            ```

    Returns:
        ComparisonLevel
            A new ComparisonLevel with the negated SQL condition and label_for_charts
    """
    cls, sql_dialect = _parse_comparison_levels(cll)
    cl = cls[0]
    result = {}
    result["sql_condition"] = f"NOT ({cl.sql_condition})"

    # Invert if is_null_level.
    # If NOT is_null_level, then we don't know if the inverted level is null or not
    if not cl.is_null_level:
        result["is_null_level"] = False

    result["label_for_charts"] = (
        label_for_charts if label_for_charts else f"NOT ({cl.label_for_charts})"
    )

    if m_probability:
        result["m_probability"] = m_probability

    return ComparisonLevel(result, sql_dialect=sql_dialect)


def _cl_merge(
    *clls: ComparisonLevel | dict,
    clause: str,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
    is_null_level: bool | None = None,
) -> ComparisonLevel:
    if len(clls) == 0:
        raise ValueError("Must provide at least one ComparisonLevel")

    cls, sql_dialect = _parse_comparison_levels(*clls)
    result = {}
    conditions = ("(" + cl.sql_condition + ")" for cl in cls)
    result["sql_condition"] = f" {clause} ".join(conditions)

    # Set to null level if all supplied levels are "null levels"
    if is_null_level is None:
        if all(d.is_null_level for d in cls):
            result["is_null_level"] = True

    if label_for_charts:
        result["label_for_charts"] = label_for_charts
    else:
        labels = ("(" + cl.label_for_charts + ")" for cl in cls)
        result["label_for_charts"] = f" {clause} ".join(labels)

    if m_probability:
        result["m_probability"] = m_probability

    return ComparisonLevel(result, sql_dialect=sql_dialect)


def br_and_(
    *brls: BlockingRule | dict | str,
    salting_partitions=1,
) -> BlockingRule:
    return _br_merge(
        *brls,
        clause="AND",
        salting_partitions=salting_partitions,
    )


def br_or_(
    *brls: BlockingRule | dict | str,
    salting_partitions: int = 1,
) -> BlockingRule:
    """Merge BlockingRules using logical "AND".

    Merge multiple BlockingRules into a single BlockingRule by
    merging their SQL conditions using a logical "AND".

    By default, we generate a new `label_for_charts` for the new BlockingRule.
    You can override this, and any other ComparisonLevel attributes, by passing
    them as keyword arguments.

    Args:
        *brls (BlockingRule | dict | str): BlockingRules or
            blocking rules in the string/dictionary format.
        salting_partitions (optional, int):

    Examples:
        === "DuckDB"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.duckdb.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.duckdb.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Spark"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.spark.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.spark.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "Athena"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.athena.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```
            Composing a levenshtein level with a custom `contains` level
            ``` python
            import splink.athena.comparison_level_library as cll
            misspelling = cll.levenshtein_level("name", 1)
            contains = {
                "sql_condition": "(contains(name_l, name_r) OR " \
                "contains(name_r, name_l))"
            }
            merged = cll.and_(misspelling, contains, label_for_charts="Spelling error")
            ```
            ```python
            merged.as_dict()
            ```
            >{
            > 'sql_condition': '(levenshtein("name_l", "name_r") <= 1) ' \
            >  'AND ((contains(name_l, name_r) OR contains(name_r, name_l)))',
            >  'label_for_charts': 'Spelling error'
            >}
        === "SQLite"
            Simple null level composition with an `AND` clause
            ``` python
            import splink.sqlite.comparison_level_library as cll
            cll.and_(cll.null_level("first_name"), cll.null_level("surname"))
            ```

    Returns:
        ComparisonLevel: A new ComparisonLevel with the merged
            SQL condition
    """
    return _br_merge(
        *brls,
        clause="OR",
        salting_partitions=salting_partitions,
    )


def br_not_(
    *brls: BlockingRule | dict | str, salting_partitions: int = 1
) -> BlockingRule:
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


def _parse_comparison_levels(
    *cls: ComparisonLevel | dict,
) -> tuple[list[ComparisonLevel], str | None]:
    cls = [_to_comparison_level(cl) for cl in cls]
    sql_dialect = _unify_sql_dialects(cls)
    return cls, sql_dialect


def _parse_blocking_rules(
    *brs: BlockingRule | dict | str,
) -> tuple[list[BlockingRule], str | None]:
    brs = [_to_blocking_rule(br) for br in brs]
    sql_dialect = _unify_sql_dialects(brs)
    salting_partitions = max([br.salting_partitions for br in brs])
    return brs, sql_dialect, salting_partitions


def _to_comparison_level(cl: ComparisonLevel | dict) -> ComparisonLevel:
    if isinstance(cl, ComparisonLevel):
        return cl
    else:
        return ComparisonLevel(cl)


def _to_blocking_rule(br):
    return blocking_rule_to_obj(br)


def _unify_sql_dialects(cls: Iterable[ComparisonLevel]) -> str | None:
    sql_dialects = set(cl.sql_dialect for cl in cls)
    sql_dialects.discard(None)
    if len(sql_dialects) > 1:
        raise ValueError("Cannot combine comparison levels with different SQL dialects")
    elif len(sql_dialects) == 0:
        return None
    return sql_dialects.pop()

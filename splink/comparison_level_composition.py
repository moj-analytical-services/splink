from __future__ import annotations

from .comparison_level import ComparisonLevel
from .composition_helpers import _and_, _not_, _or_


def and_(
    *clls: ComparisonLevel | dict,
    label_for_charts=None,
    m_probability=None,
    is_null_level=None,
    **kwargs,
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
    return _and_(
        *clls,
        label_for_charts=label_for_charts,
        m_probability=m_probability,
        is_null_level=is_null_level,
        **kwargs,
    )


def or_(
    *clls: ComparisonLevel | dict,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
    is_null_level: bool | None = None,
    **kwargs,
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

    return _or_(
        *clls,
        label_for_charts=label_for_charts,
        m_probability=m_probability,
        is_null_level=is_null_level,
        **kwargs,
    )


def not_(
    cll: ComparisonLevel | dict,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
    **kwargs,
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

    return _not_(
        cll,
        class_name="ComparisonLevel",
        label_for_charts=label_for_charts,
        m_probability=m_probability,
        **kwargs,
    )

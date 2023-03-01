from __future__ import annotations

from typing import Iterable

from splink.comparison_level import ComparisonLevel

_DOCSTRING_TEMPLATE = """
Merge ComparisonLevels using logical "{clause}".

Merge multiple ComparisonLevels into a single ComparisonLevel by
merging their SQL conditions using a logical "{clause}".

By default, we generate a new ``label_for_charts`` for the new ComparisonLevel.
You can override this, and any other ComparisonLevel attributes, by passing
them as keyword arguments.

Parameters
----------
*clls
    ComparisonLevels or dicts to merge
**overrides
    Any attributes of the new ComparisonLevel that you want to override

Returns
-------
ComparisonLevel
    A new ComparisonLevel with the merged SQL condition and label_for_charts

Examples
--------
>>> import splink.duckdb.duckdb_comparison_level_library as cll
>>> misspelling = cll.levenshtein_level("name", 1)
>>> contains = {{
...     "sql_condition": "(contains(name_l, name_r) OR contains(name_r, name_l))"
... }}
>>> merged = cl_{clause}(misspelling, contains, label_for_charts="Spelling error")
>>> merged.as_dict()
{{
    'sql_condition': '(levenshtein("name_l", "name_r") <= 1) OR ((contains(name_l, name_r) OR contains(name_r, name_l)))',
    'label_for_charts': 'Spelling error'
}}
""".strip()  # noqa: E501


def cl_and(*clls: ComparisonLevel | dict, **overrides) -> ComparisonLevel:
    return _cl_merge(*clls, clause="AND", **overrides)


def cl_or(*clls: ComparisonLevel | dict, **overrides) -> ComparisonLevel:
    return _cl_merge(*clls, clause="OR", **overrides)


cl_and.__doc__ = _DOCSTRING_TEMPLATE.format(clause="and")
cl_or.__doc__ = _DOCSTRING_TEMPLATE.format(clause="of")


def cl_not(cll: ComparisonLevel | dict, **overrides) -> ComparisonLevel:
    """Negate a ComparisonLevel.

    Returns a ComparisonLevel with the same SQL condition as the input,
    but prefixed with "NOT".

    By default, we generate a new ``label_for_charts`` for the new ComparisonLevel.
    You can override this, and any other ComparisonLevel attributes, by passing
    them as keyword arguments.

    Parameters
    ----------
    cll
        ComparisonLevel or dict to negate
    **overrides
        Any attributes of the new ComparisonLevel that you want to override

    Returns
    -------
    ComparisonLevel
        A new ComparisonLevel with the negated SQL condition and label_for_charts
    """
    dicts, sql_dialect = _parse_comparison_levels(cll)
    result = {**overrides}
    cld = dicts[0]
    result["sql_condition"] = f"NOT ({cld['sql_condition']})"
    if "label_for_charts" not in result:
        result["label_for_charts"] = f"NOT ({_label_for_charts(cld)})"
    return ComparisonLevel(result, sql_dialect=sql_dialect)


def _cl_merge(
    *clls: ComparisonLevel | dict, clause: str, **overrides
) -> ComparisonLevel:
    if len(clls) == 0:
        raise ValueError("Must provide at least one ComparisonLevel")
    dicts, sql_dialect = _parse_comparison_levels(*clls)
    result = {**overrides}
    conditions = ("(" + d["sql_condition"] + ")" for d in dicts)
    result["sql_condition"] = f" {clause} ".join(conditions)

    # Set to null level if all supplied levels are "null levels"
    if "is_null_level" not in result:
        if all([d.setdefault('is_null_level', False) for d in dicts]):
            result["is_null_level"] = True

    if "label_for_charts" not in result:
        labels = ("(" + _label_for_charts(d) + ")" for d in dicts)
        result["label_for_charts"] = f" {clause} ".join(labels)
    return ComparisonLevel(result, sql_dialect=sql_dialect)


def _label_for_charts(comparison_dict: dict) -> str:
    backup = comparison_dict["sql_condition"]
    label = comparison_dict.get("label_for_charts", backup)

    colname = comparison_dict.get("column_name")
    if colname is None:
        return label
    # if null level, prefix with the column name
    if comparison_dict.get('is_null_level', False):
        label = f"{colname} is {label.upper()}"
    # if exact match, suffix w/ colname
    elif label.lower() == "exact match":
        label = f"{label} on {colname}"

    return label


def _parse_comparison_levels(
    *clls: ComparisonLevel | dict,
) -> tuple[list[dict], str | None]:
    cl_dicts = [_to_comparison_level_dict(cll) for cll in clls]
    sql_dialect = _unify_sql_dialects(cl_dicts)
    return cl_dicts, sql_dialect


def _to_comparison_level_dict(cl: ComparisonLevel | dict) -> dict:
    if isinstance(cl, ComparisonLevel):
        return {**cl.as_dict(), "column_name": cl.column_name}
    else:
        return cl


def _unify_sql_dialects(cls: Iterable[dict]) -> str | None:
    sql_dialects = set(cl.get("sql_dialect", None) for cl in cls)
    sql_dialects.discard(None)
    if len(sql_dialects) > 1:
        raise ValueError("Cannot combine comparison levels with different SQL dialects")
    elif len(sql_dialects) == 0:
        return None
    return sql_dialects.pop()

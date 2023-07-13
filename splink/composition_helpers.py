from __future__ import annotations

import warnings
from typing import Iterable

from .blocking import BlockingRule, blocking_rule_to_obj
from .comparison_level import ComparisonLevel


def compose_sql(*sql_classes, clause, **kwargs):

    if len(sql_classes) == 0:
        raise SyntaxError("You must provide at least one level for composition.")

    # Main wrapper for the `and_` and `or_` logic.
    # As these functions are identical outside of a differing clause,
    # it's simpler to wrap them into a single function and allow the
    # clause to be manipulated.

    clause = clause
    # check our inputs
    validate_signatures(*sql_classes, clause=clause)

    comparison_signatures = (ComparisonLevel, dict)

    if isinstance(sql_classes[0], comparison_signatures):

        # Set default kwargs
        defaults = {
            "label_for_charts": None,
            "m_probability": None,
            "is_null_level": None,
        }
        kwargs = reset_kwargs(defaults, kwargs)

        return _cl_merge(
            *sql_classes,
            clause=clause,
            **kwargs,
        )
    else:
        # Set default kwargs
        defaults = {"salting_partitions": 1}
        kwargs = reset_kwargs(defaults, kwargs)
        return _br_merge(
            *sql_classes,
            clause=clause,
            **kwargs,
        )


# Main `and_` logic
def _and_(*sql_classes, **kwargs):
    return compose_sql(*sql_classes, clause="AND", **kwargs)


# Main `or_` logic
def _or_(*sql_classes, **kwargs):
    return compose_sql(*sql_classes, clause="OR", **kwargs)


def _not_(*sql_classes, class_name, **kwargs):
    if len(sql_classes) == 0:
        raise SyntaxError("You must provide at least one level for composition.")

    if len(sql_classes) > 1:
        warning_txt = f"""
            More than one `{class_name}` entered for `NOT` composition.
            This function only accepts one argument and will only use your
            first `{class_name}`.
        """

        warnings.warning(
            warning_txt,
            SyntaxWarning,
            stacklevel=2,
        )

    level = sql_classes[0]

    return compose_sql(
        level,
        clause="NOT",
        **kwargs,
    )


##############################
### COMPARISON COMPOSITION ###
##############################
def _cl_merge(
    *clls: ComparisonLevel | dict,
    clause: str,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
    is_null_level: bool | None = None,
) -> ComparisonLevel:
    cls, sql_dialect = _parse_comparison_levels(*clls)
    conditions = ("(" + cl.sql_condition + ")" for cl in cls)

    if clause == "NOT":
        result = cl_not(
            cls[0],  # only used the first CL
            label_for_charts,
            m_probability,
        )
    else:
        result = {}
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


def cl_not(
    cl: ComparisonLevel | dict,
    label_for_charts: str | None = None,
    m_probability: float | None = None,
):
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

    return result


def _parse_comparison_levels(
    *cls: ComparisonLevel | dict,
) -> tuple[list[ComparisonLevel], str | None]:
    cls = [_to_comparison_level(cl) for cl in cls]
    sql_dialect = _unify_sql_dialects(cls)
    return cls, sql_dialect


def _to_comparison_level(cl: ComparisonLevel | dict) -> ComparisonLevel:
    if isinstance(cl, ComparisonLevel):
        return cl
    else:
        return ComparisonLevel(cl)


#################################
### BLOCKING RULE COMPOSITION ###
#################################
def _br_merge(
    *brls: BlockingRule | str,
    clause: str,
    salting_partitions: int = 1,
) -> BlockingRule:
    brs, sql_dialect, salt = _parse_blocking_rules(*brls)
    conditions = (f"({br.blocking_rule})" for br in brs)

    if clause == "NOT":
        br = brs[0]
        blocking_rule = f"NOT ({br.blocking_rule})"
    else:
        blocking_rule = f" {clause} ".join(conditions)

    return BlockingRule(
        blocking_rule,
        salting_partitions=salting_partitions if salting_partitions > 1 else salt,
        sql_dialect=sql_dialect,
    )


def _parse_blocking_rules(
    *brs: BlockingRule | str,
) -> tuple[list[BlockingRule], str | None]:
    brs = [_to_blocking_rule(br) for br in brs]
    sql_dialect = _unify_sql_dialects(brs)
    salting_partitions = max([br.salting_partitions for br in brs])
    return brs, sql_dialect, salting_partitions


def _to_blocking_rule(br):
    return blocking_rule_to_obj(br)


########################
### HELPER FUNCTIONS ###
########################
def _unify_sql_dialects(
    cls: Iterable[ComparisonLevel] | Iterable[BlockingRule],
) -> str | None:
    sql_dialects = set(cl.sql_dialect for cl in cls)
    sql_dialects.discard(None)
    if len(sql_dialects) > 1:
        raise ValueError("Cannot combine comparison levels with different SQL dialects")
    elif len(sql_dialects) == 0:
        return None
    return sql_dialects.pop()


def validate_signatures(*args, clause):
    comparison_signatures = (ComparisonLevel, dict)
    blocking_signatures = (BlockingRule, str)
    valid_signatures = comparison_signatures + blocking_signatures

    # check if any invalid types have been passed
    checks = [isinstance(arg, valid_signatures) for arg in args]
    if not all(checks):
        # needs some final logic around how
        raise TypeError(
            f"Error: `{clause.lower()}_` only accepts `ComparisonLevel` or `dict` types"
            "for Comparison level composition, and `BlockingRule` and `str` types for "
            "BlockingRule composition as arbitrary arguments. \n\nFor additional "
            "functionality please ensure you add the key word argument to the "
            "function call."
        )

    # check if CL and BR args have been passed together
    comparison_signatures = (ComparisonLevel, dict)
    blocking_signatures = (BlockingRule, str)

    # Check if comparison signatures are detected
    comparison_args_valid = any(isinstance(arg, comparison_signatures) for arg in args)
    # Check if blocking signatures are detected
    blocking_args_valid = any(isinstance(arg, blocking_signatures) for arg in args)

    # If both types of signature are detected, error out
    if comparison_args_valid and blocking_args_valid:
        raise TypeError(
            "Error: conflicting data types detected for "
            "`{clause.lower()}_` composition. "
            "Please ensure that only `ComparisonLevel` or `dict` types are passed if "
            "you're composing comparison levels and `BlockingRule` and `str` types for "
            "Blocking Rule composition."
        )


def reset_kwargs(defaults, kwargs):
    return {key: kwargs.get(key, value) for key, value in defaults.items()}

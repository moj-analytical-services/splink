import pytest

from splink.internals.blocking_rule_library import And, CustomRule, Not, Or, block_on
from splink.internals.input_column import _get_dialect_quotes

from .decorator import mark_with_dialects_excluding


def binary_composition_internals(clause, comp_fun, dialect):
    q, _ = _get_dialect_quotes(dialect)

    # Test what happens when only one value is fed
    # It should just report the regular outputs of our comparison level func
    level = comp_fun(block_on("tom")).get_blocking_rule(dialect)
    assert level.blocking_rule_sql == f"(l.{q}tom{q} = r.{q}tom{q})"

    # Exact match and null level composition
    level = comp_fun(
        block_on("first_name"),
        block_on("surname"),
    ).get_blocking_rule(dialect)
    exact_match_sql = f"(l.{q}first_name{q} = r.{q}first_name{q}) {clause} (l.{q}surname{q} = r.{q}surname{q})"  # noqa: E501
    assert level.blocking_rule_sql == exact_match_sql
    # not_(or_(...)) composition
    level = Not(
        comp_fun(block_on("first_name"), block_on("surname")),
    ).get_blocking_rule(dialect)
    assert level.blocking_rule_sql == f"NOT ({exact_match_sql})"

    # Check salting outputs
    # salting included in the composition function
    salt = (
        comp_fun(
            CustomRule("l.help2 = r.help2"),
            CustomRule("l.help3 = r.help3", salting_partitions=10),
            block_on("help4"),
            salting_partitions=4,
        ).get_blocking_rule(dialect)
    ).salting_partitions

    # salting included in one of the levels
    salt_2 = comp_fun(
        CustomRule("l.help2 = r.help2"),
        CustomRule("l.help3 = r.help3", salting_partitions=3),
        block_on("help4"),
    ).salting_partitions

    assert salt == 4
    assert salt_2 == 3

    with pytest.raises(ValueError):
        comp_fun()


@mark_with_dialects_excluding()
def test_binary_composition_internals_OR(test_helpers, dialect):
    binary_composition_internals("OR", Or, dialect)


@mark_with_dialects_excluding()
def test_binary_composition_internals_AND(test_helpers, dialect):
    binary_composition_internals("AND", And, dialect)


def test_not():
    # Integration test for a simple dictionary blocking rule
    dob_jan_first = {"blocking_rule": "SUBSTR(dob_std_l, -5) = '01-01'"}
    Not(dob_jan_first)

    with pytest.raises(TypeError):
        Not()

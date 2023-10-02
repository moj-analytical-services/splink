import pytest

from splink.input_column import _get_dialect_quotes

from .decorator import mark_with_dialects_excluding


def binary_composition_internals(clause, comp_fun, brl, dialect):
    q, _ = _get_dialect_quotes(dialect)

    # Test what happens when only one value is fed
    # It should just report the regular outputs of our comparison level func
    level = comp_fun(brl.exact_match_rule("tom"))
    assert level.blocking_rule == f"l.{q}tom{q} = r.{q}tom{q}"

    # Exact match and null level composition
    level = comp_fun(
        brl.exact_match_rule("first_name"),
        brl.exact_match_rule("surname"),
    )
    exact_match_sql = f"(l.{q}first_name{q} = r.{q}first_name{q}) {clause} (l.{q}surname{q} = r.{q}surname{q})"  # noqa: E501
    assert level.blocking_rule == exact_match_sql
    # brl.not_(or_(...)) composition
    level = brl.not_(
        comp_fun(brl.exact_match_rule("first_name"), brl.exact_match_rule("surname")),
    )
    assert level.blocking_rule == f"NOT ({exact_match_sql})"

    # Check salting outputs
    # salting included in the composition function
    salt = comp_fun(
        "l.help2 = r.help2",
        {"blocking_rule": "l.help3 = r.help3", "salting_partitions": 10},
        brl.exact_match_rule("help4"),
        salting_partitions=4,
    ).salting_partitions

    # salting included in one of the levels
    salt_2 = comp_fun(
        "l.help2 = r.help2",
        {"blocking_rule": "l.help3 = r.help3", "salting_partitions": 3},
        brl.exact_match_rule("help4"),
    ).salting_partitions

    assert salt == 4
    assert salt_2 == 3

    with pytest.raises(ValueError):
        comp_fun()


@mark_with_dialects_excluding()
def test_binary_composition_internals_OR(test_helpers, dialect):
    brl = test_helpers[dialect].brl
    binary_composition_internals("OR", brl.or_, brl, dialect)


@mark_with_dialects_excluding()
def test_binary_composition_internals_AND(test_helpers, dialect):
    brl = test_helpers[dialect].brl
    binary_composition_internals("AND", brl.and_, brl, dialect)


def test_not():
    import splink.duckdb.duckdb_comparison_level_library as brl

    # Integration test for a simple dictionary cl
    dob_jan_first = {"sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'"}
    brl.not_(dob_jan_first)

    with pytest.raises(TypeError):
        brl.not_()

import pytest

from splink.input_column import _get_dialect_quotes

from .decorator import mark_with_dialects_excluding


def binary_composition_internals(clause, comp_fun, brl, dialect):
    q, _ = _get_dialect_quotes(dialect)

    # Test what happens when only one value is fed
    # It should just report the regular outputs of our comparison level func
    level = comp_fun(brl.exact_match_rule("tom"))
    assert level.blocking_rule == f"(l.{q}tom{q} = r.{q}tom{q})"

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
        brl.exact_match_rule("help4"),
        salting_partitions=4,
    ).salting_partitions

    assert salt == 4

    with pytest.raises(SyntaxError):
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
    import splink.duckdb.blocking_rule_library as brl
    import splink.duckdb.comparison_level_library as cl

    # Integration test for a simple string BR
    dob_jan_first = "SUBSTR(dob_std_l, -5) = '01-01'"
    cl.not_(dob_jan_first)  # should stil work
    out = cl.not_(dob_jan_first, salting_partitions=5)  # should stil work
    assert out.salting_partitions == 5

    brl.not_(dob_jan_first)

    with pytest.raises(TypeError):
        brl.not_()


def test_error_on_multiple_input_types():
    # Check that opposing input types cannot be merged
    # i.e. `and_(ComparisonLevel, BlockingRule)`` should fail

    import splink.duckdb.blocking_rule_library as brl
    import splink.duckdb.comparison_level_library as cll

    error_txt = (
        "Error: conflicting data types detected for `{}_` composition. "
        "Please ensure that only `ComparisonLevel` or `dict` types are "
        "passed if you're composing comparison levels and `BlockingRule` "
        "and `str` types for Blocking Rule composition."
    )

    or_error = error_txt.format("or")
    and_error = error_txt.format("and")

    cll_test = cll.or_(
        cll.exact_match_level("first_name", include_colname_in_charts_label=True),
        cll.exact_match_level("surname"),
    )
    br_test = brl.exact_match_rule("first_name")

    # We're expecting a TypeError
    with pytest.raises(TypeError) as excinfo:
        cll.or_(cll_test, br_test)
    assert str(excinfo.value) == or_error

    with pytest.raises(TypeError) as excinfo:
        brl.and_(cll_test, br_test)
    assert str(excinfo.value) == and_error

    contains = {
        "sql_condition": "(contains(name_l, name_r) OR contains(name_r, name_l))"
    }
    with pytest.raises(TypeError):
        brl.or_(contains, br_test)

    dob_jan_first = "SUBSTR(dob_std_l, -5) = '01-01'"
    with pytest.raises(TypeError):
        cll.and_(cll_test, dob_jan_first)

import pandas as pd
import pytest

import splink.internals.comparison_level_library as cll
from splink.internals.input_column import _get_dialect_quotes

from .decorator import mark_with_dialects_excluding


def binary_composition_internals(clause, c_fun, dialect, q):
    # Test what happens when only one value is fed
    # It should just report the regular outputs of our comparison level func
    level = c_fun(
        cll.ExactMatchLevel(
            "tom",
            # include_colname_in_charts_label=True
        )
    ).get_comparison_level(dialect)
    assert level.sql_condition == f"({q}tom_l{q} = {q}tom_r{q})"
    assert level.label_for_charts == "(Exact match on tom)"

    # Two null levels composed
    level = (
        c_fun(
            cll.NullLevel("first_name"),
            cll.NullLevel("surname"),
        )
        .configure(
            label_for_charts="This is a test",
        )
        .get_comparison_level(dialect)
    )

    null_sql = (
        f"({q}first_name_l{q} IS NULL OR {q}first_name_r{q} IS NULL) {clause} "
        f"({q}surname_l{q} IS NULL OR {q}surname_r{q} IS NULL)"
    )
    assert level.sql_condition == null_sql
    # Default label
    assert level.label_for_charts == "This is a test"
    # As both inputs are null, we're expecting this to return True
    assert level.is_null_level is True

    # Exact match and null level composition
    level = (
        c_fun(
            cll.ExactMatchLevel(
                "first_name",
                # include_colname_in_charts_label=True
            ),
            cll.NullLevel("first_name"),
        )
        .configure(m_probability=0.5)
        .get_comparison_level(dialect)
    )
    assert (
        level.sql_condition == f"({q}first_name_l{q} = {q}first_name_r{q}) {clause} "
        f"({q}first_name_l{q} IS NULL OR {q}first_name_r{q} IS NULL)"
    )
    # Default label
    assert level.label_for_charts == (
        "(Exact match on first_name) " f"{clause} " "(first_name is NULL)"
    )
    # should default to False
    assert level.is_null_level is False
    assert level._m_probability == 0.5

    # cll.Not(or_(...)) composition
    level = (
        cll.Not(
            c_fun(cll.ExactMatchLevel("first_name"), cll.ExactMatchLevel("surname"))
        )
        .configure(m_probability=0.5)
        .get_comparison_level(dialect)
    )

    exact_match_sql = f"({q}first_name_l{q} = {q}first_name_r{q}) {clause} ({q}surname_l{q} = {q}surname_r{q})"  # noqa: E501
    assert level.sql_condition == f"NOT ({exact_match_sql})"

    with pytest.raises(ValueError):
        c_fun()


@mark_with_dialects_excluding()
def test_binary_composition_internals_OR(dialect):
    quo, _ = _get_dialect_quotes(dialect)
    binary_composition_internals("OR", cll.Or, dialect, quo)


@mark_with_dialects_excluding()
def test_binary_composition_internals_AND(dialect):
    quo, _ = _get_dialect_quotes(dialect)
    binary_composition_internals("AND", cll.And, dialect, quo)


def test_not():
    level = cll.Not(cll.NullLevel("first_name")).get_comparison_level("duckdb")
    assert level.is_null_level is False

    # Integration test for a simple dictionary cl
    dob_jan_first = {"sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'"}
    cll.Not(dob_jan_first)

    with pytest.raises(TypeError):
        cll.Not()


@mark_with_dialects_excluding()
def test_null_level_composition(dialect):
    c = (
        cll.And(cll.NullLevel("first_name"), cll.NullLevel("surname")).configure(
            is_null_level=True
        )
    ).get_comparison_level(dialect)
    assert c.is_null_level

    c = (
        cll.And(
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("surname"),
        )
        .configure(is_null_level=True)
        .get_comparison_level(dialect)
    )
    assert c.is_null_level

    c = cll.And(
        cll.NullLevel("first_name"), cll.NullLevel("surname")
    ).get_comparison_level(dialect)
    assert c.is_null_level

    c = (
        cll.Or(cll.NullLevel("first_name"), cll.NullLevel("surname"))
        .configure(is_null_level=True)
        .get_comparison_level(dialect)
    )
    assert c.is_null_level

    c = (
        cll.Or(
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("surname"),
        )
        .configure(is_null_level=True)
        .get_comparison_level(dialect)
    )
    assert c.is_null_level

    c = cll.Or(
        cll.NullLevel("first_name"), cll.NullLevel("surname")
    ).get_comparison_level(dialect)
    assert c.is_null_level


@mark_with_dialects_excluding()
def test_composition_outputs(test_helpers, dialect):
    helper = test_helpers[dialect]

    # Check our compositions give expected outputs
    df = pd.DataFrame(
        [
            {"unique_id": 1, "forename": "Tom", "surname": "Tim"},
            {"unique_id": 2, "forename": "Tom", "surname": "Tim"},
            {"unique_id": 3, "forename": "Tom", "surname": "Timothee"},
            {"unique_id": 4, "forename": "Sam", "surname": "Tarly"},
            {"unique_id": 5, "forename": "Sam", "surname": "Tim"},
        ]
    )

    # For testing the cll version
    dbl_null = cll.Or(cll.NullLevel("forename"), cll.NullLevel("surname"))
    both = cll.And(cll.ExactMatchLevel("forename"), cll.ExactMatchLevel("surname"))
    either = cll.Or(cll.ExactMatchLevel("forename"), cll.ExactMatchLevel("surname"))

    full_name = {
        "output_column_name": "full_name",
        "comparison_levels": [
            dbl_null,
            both,
            either,
            cll.Not(both),  # acts as an "else" level
            cll.ElseLevel(),
        ],
    }

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [full_name],
    }

    linker = helper.Linker(df, settings, **helper.extra_linker_args())

    pred = linker.inference.predict()
    out = pred.as_pandas_dataframe().sort_values(by=["unique_id_l", "unique_id_r"])

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        3: [(1, 2)],
        2: [(1, 3), (1, 5), (2, 3), (2, 5), (4, 5)],
        1: [(1, 4), (2, 4), (3, 4), (3, 5)],
    }

    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            row = out.loc[(out.unique_id_l == left) & (out.unique_id_r == right)]
            assert row["gamma_full_name"].values[0] == gamma

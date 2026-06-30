from pytest import mark, raises

from splink.internals.comparison_level import ComparisonLevel
from splink.internals.dialects import SplinkDialect

from .decorator import mark_with_dialects_excluding


def make_comparison_level(sql_condition, dialect):
    sql_dialect = SplinkDialect.from_string(dialect)
    return ComparisonLevel(
        sql_condition=sql_condition,
        label_for_charts="nice_informative_label",
        sql_dialect=sql_dialect,
    )


# SQL conditions that are of 'exact match' type
exact_matchy_sql_conditions_and_columns = [
    ("col_l = col_r", {"col"}),
    ("col_l = col_r AND another_col_l = another_col_r", {"col", "another_col"}),
    (
        "col_l = col_r AND another_col_l = another_col_r AND third_l = third_r",
        {"col", "another_col", "third"},
    ),
    (
        "(col_l = col_r AND another_col_l = another_col_r) AND third_l = third_r",
        {"col", "another_col", "third"},
    ),
    (
        "col_l = col_r AND (another_col_l = another_col_r AND third_l = third_r)",
        {"col", "another_col", "third"},
    ),
]


@mark.parametrize(
    "sql_condition, exact_match_cols", exact_matchy_sql_conditions_and_columns
)
@mark_with_dialects_excluding()
def test_is_exact_match_for_exact_matchy_levels(
    sql_condition, exact_match_cols, dialect
):
    lev = make_comparison_level(sql_condition, dialect)
    assert lev._is_exact_match


@mark.parametrize(
    "sql_condition, exact_match_cols", exact_matchy_sql_conditions_and_columns
)
@mark_with_dialects_excluding()
def test_exact_match_colnames_for_exact_matchy_levels(
    sql_condition, exact_match_cols, dialect
):
    lev = make_comparison_level(sql_condition, dialect)
    assert set(lev._exact_match_colnames) == exact_match_cols


# SQL conditions that are NOT of 'exact match' type
non_exact_matchy_sql_conditions = [
    "levenshtein(col_l, col_r) < 3",
    "col_l < col_r",
    "col_l = col_r OR another_col_l = another_col_r",
    "col_l = a_different_col_r",
    "col_l = col_r AND (col_2_l = col_2_r OR col_3_l = col_3_r)",
    "col_l = col_r AND (col_2_l < col_2_r)",
    "substr(col_l, 2) = substr(col_r, 2)",
]


@mark.parametrize("sql_condition", non_exact_matchy_sql_conditions)
@mark_with_dialects_excluding()
def test_is_exact_match_for_non_exact_matchy_levels(sql_condition, dialect):
    lev = make_comparison_level(sql_condition, dialect)
    assert not lev._is_exact_match


@mark.parametrize("sql_condition", non_exact_matchy_sql_conditions)
@mark_with_dialects_excluding()
def test_exact_match_colnames_for_non_exact_matchy_levels(sql_condition, dialect):
    lev = make_comparison_level(sql_condition, dialect)
    # _exact_match_colnames should have an error if it is
    # not actually an exact match level
    with raises(ValueError):
        lev._exact_match_colnames  # noqa: B018


def _fixed_weight_level(fixed_match_weight, **kwargs):
    return ComparisonLevel(
        sql_condition="ELSE",
        label_for_charts="nice_informative_label",
        sql_dialect=SplinkDialect.from_string("duckdb"),
        fixed_match_weight=fixed_match_weight,
        **kwargs,
    )


def test_fixed_match_weight_positive_serialisation():
    lev = _fixed_weight_level(4)

    # A positive match weight pins m to 1.0 and encodes the weight in u
    assert lev.fixed_match_weight == 4
    assert lev.m_probability == 1.0
    assert lev.u_probability == 2**-4
    assert lev._match_weight == 4
    assert lev._log2_bayes_factor == 4
    assert lev._bayes_factor == 2**4

    lev_dict = lev.as_dict()
    # Only the fixed match weight is serialised - no derived m/u probabilities
    assert lev_dict["fixed_match_weight"] == 4
    assert "m_probability" not in lev_dict
    assert "u_probability" not in lev_dict


def test_fixed_match_weight_negative_serialisation():
    lev = _fixed_weight_level(-4)

    # A negative match weight pins u to 1.0 and encodes the weight in m
    assert lev.fixed_match_weight == -4
    assert lev.m_probability == 2**-4
    assert lev.u_probability == 1.0
    assert lev._match_weight == -4
    assert lev._log2_bayes_factor == -4

    lev_dict = lev.as_dict()
    assert lev_dict["fixed_match_weight"] == -4
    assert "m_probability" not in lev_dict
    assert "u_probability" not in lev_dict


def test_fixed_match_weight_zero():
    lev = _fixed_weight_level(0)
    assert lev.m_probability == 1.0
    assert lev.u_probability == 1.0
    assert lev._match_weight == 0
    assert lev.as_dict()["fixed_match_weight"] == 0


def test_fixed_match_weight_round_trips_through_as_dict():
    lev = _fixed_weight_level(2.5)
    rebuilt = ComparisonLevel(
        sql_dialect=SplinkDialect.from_string("duckdb"),
        **lev.as_dict(),
    )
    assert rebuilt.fixed_match_weight == 2.5
    assert rebuilt._match_weight == lev._match_weight
    assert rebuilt.m_probability == lev.m_probability
    assert rebuilt.u_probability == lev.u_probability


def test_fixed_match_weight_rejects_m_probability():
    with raises(ValueError):
        _fixed_weight_level(-2, m_probability=0.2)


def test_fixed_match_weight_rejects_u_probability():
    with raises(ValueError):
        _fixed_weight_level(-2, u_probability=0.8)


def test_fixed_match_weight_rejects_on_null_level():
    with raises(ValueError):
        _fixed_weight_level(3, is_null_level=True)


def test_fixed_match_weight_rejects_non_finite():
    with raises(ValueError):
        _fixed_weight_level(float("inf"))

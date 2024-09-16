from pytest import mark, raises

from splink.internals.comparison_level import ComparisonLevel

from .decorator import mark_with_dialects_excluding


def make_comparison_level(sql_condition, dialect):
    return ComparisonLevel(
        sql_condition=sql_condition,
        label_for_charts="nice_informative_label",
        sqlglot_dialect=dialect,
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

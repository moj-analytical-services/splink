import pandas as pd
import pytest

import splink.duckdb.blocking_rule_library as brl
import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_level_library as scll
from splink.duckdb.linker import DuckDBLinker


def test_not():
    level = cll.not_(cll.null_level("first_name"))
    assert level.is_null_level is False

    # Integration test for a simple dictionary cl
    dob_jan_first = {"sql_condition": "SUBSTR(dob_std_l, -5) = '01-01'"}
    cll.not_(dob_jan_first)

    with pytest.raises(TypeError):
        cll.not_()


@pytest.mark.parametrize(
    ("clause", "c_fun"),
    [
        pytest.param("OR", cll.or_, id="Test or_"),
        pytest.param("OR", brl.or_, id="Test blocking rule or_"),
        pytest.param("AND", cll.and_, id="Test and_"),
        pytest.param("OR", scll.or_, id="Test spark or_"),
    ],
)
def test_binary_composition_internals(clause, c_fun):
    # Test what happens when only one value is fed
    # It should just report the regular outputs of our comparison level func
    level = c_fun(cll.exact_match_level("tom", include_colname_in_charts_label=True))
    assert level.sql_condition == '("tom_l" = "tom_r")'
    assert level.label_for_charts == "(Exact match tom)"

    # Two null levels composed
    level = c_fun(
        cll.null_level("first_name"),
        cll.null_level("surname"),
        label_for_charts="This is a test",
    )

    null_sql = (
        f'("first_name_l" IS NULL OR "first_name_r" IS NULL) {clause} '
        '("surname_l" IS NULL OR "surname_r" IS NULL)'
    )
    assert level.sql_condition == null_sql
    # Default label
    assert level.label_for_charts == "This is a test"
    # As both inputs are null, we're expecting this to return True
    assert level.is_null_level is True

    # Exact match and null level composition
    level = c_fun(
        cll.exact_match_level("first_name", include_colname_in_charts_label=True),
        cll.null_level("first_name"),
        m_probability=0.5,
    )
    assert (
        level.sql_condition == f'("first_name_l" = "first_name_r") {clause} '
        '("first_name_l" IS NULL OR "first_name_r" IS NULL)'
    )
    # Default label
    assert level.label_for_charts == f"(Exact match first_name) {clause} (Null)"
    # should default to False
    assert level.is_null_level is False
    assert level._m_probability == 0.5

    # cll.not_(or_(...)) composition
    level = cll.not_(
        c_fun(cll.exact_match_level("first_name"), cll.exact_match_level("surname")),
        m_probability=0.5,
    )

    exact_match_sql = (
        f'("first_name_l" = "first_name_r") {clause} ("surname_l" = "surname_r")'
    )
    assert level.sql_condition == f"NOT ({exact_match_sql})"

    with pytest.raises(SyntaxError):
        c_fun()


def test_composition_outputs():
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
    dbl_null = cll.or_(cll.null_level("forename"), cll.null_level("surname"))
    both = cll.and_(cll.exact_match_level("forename"), cll.exact_match_level("surname"))
    either = cll.or_(
        cll.exact_match_level("forename"), cll.exact_match_level("surname")
    )

    full_name = {
        "output_column_name": "full_name",
        "comparison_levels": [
            dbl_null,
            both,
            either,
            cll.not_(both),  # acts as an "else" level
            cll.else_level(),
        ],
    }

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [full_name],
    }

    linker = DuckDBLinker(df, settings)

    pred = linker.predict()
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

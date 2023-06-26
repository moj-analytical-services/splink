import pandas as pd
from sqlglot import parse_one
from sqlglot.optimizer.normalize import normalize

import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_library as cl
from splink.duckdb.linker import DuckDBLinker


def test_compound_comparison_level():
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "John",
                "middle_name": "James",
                "surname": "Smith",
                "city": "Brighton",
            },
            {
                "unique_id": 2,
                "first_name": "Mary",
                "middle_name": "Harriet",
                "surname": "Jones",
                "city": "Brighton",
            },
            {
                "unique_id": 3,
                "first_name": "Jane",
                "middle_name": "Joan",
                "surname": "Taylor",
                "city": "Brighton",
            },
            {
                "unique_id": 4,
                "first_name": "John",
                "middle_name": "Blake",
                "surname": "Jones",
                "city": "Brighton",
            },
            {
                "unique_id": 5,
                "first_name": "Jane",
                "middle_name": "Joan",
                "surname": "Taylor",
                "city": "Brighton",
            },
            {
                "unique_id": 6,
                "first_name": "Gill",
                "middle_name": "Harriet",
                "surname": "Greene",
                "city": "Brighton",
            },
            {
                "unique_id": 7,
                "first_name": "Owen",
                "middle_name": "James",
                "surname": "Smith",
                "city": "Brighton",
            },
            {
                "unique_id": 8,
                "first_name": "Sarah",
                "middle_name": "Simone",
                "surname": "Williams",
                "city": "Brighton",
            },
        ]
    )

    def col_is_match(col):
        return f"({col}_l = {col}_r)"

    def col_is_null(col):
        return f"({col}_l IS NULL OR {col}_r IS NULL)"

    sql_and_clauses_joined_with_ors = (
        f"(({col_is_match('first_name')} AND {col_is_match('middle_name')}) OR "
        f"({col_is_match('middle_name')} AND {col_is_match('surname')}) OR "
        f"({col_is_match('surname')} AND {col_is_match('first_name')}))"
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.exact_match("city"),
            {
                "output_column_name": "company_comparison",
                "comparison_levels": [
                    {
                        "sql_condition": (
                            f"{col_is_null('first_name')} AND "
                            f"{col_is_null('middle_name')} AND "
                            f"{col_is_null('surname')}"
                        ),
                        "label_for_charts": "NULL",
                        "is_null_level": True,
                    },
                    # ignoring other permutations of NULL
                    {
                        "sql_condition": (
                            f"{col_is_match('first_name')} AND "
                            f"{col_is_match('middle_name')} AND "
                            f"{col_is_match('surname')}"
                        ),
                        "label_for_charts": "All three match",
                    },
                    {
                        "sql_condition": sql_and_clauses_joined_with_ors,
                        "label_for_charts": "2 out of 3 columns match",
                    },
                    cll.exact_match_level("first_name"),
                    cll.exact_match_level("middle_name"),
                    cll.exact_match_level("surname"),
                    cll.else_level(),
                ],
            },
        ],
    }

    linker = DuckDBLinker(df, settings)
    all_cols_match_level = linker._settings_obj.comparisons[1].comparison_levels[1]
    assert all_cols_match_level._is_exact_match
    assert set(all_cols_match_level._exact_match_colnames) == {
        "first_name",
        "middle_name",
        "surname",
    }

    linker.estimate_parameters_using_expectation_maximisation("l.city = r.city")


def test_complex_compound_comparison_level():
    # non-realistic example
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "col_1": "a",
                "col_2": "b",
                "col_3": "c",
                "col_4": "d",
                "col_5": "e",
                "col_6": "f",
                "col_7": "g",
            },
            {
                "unique_id": 2,
                "col_1": "aa",
                "col_2": "b",
                "col_3": "cc",
                "col_4": "d",
                "col_5": "ee",
                "col_6": "f",
                "col_7": "gg",
            },
            {
                "unique_id": 3,
                "col_1": "a",
                "col_2": "bb",
                "col_3": "c",
                "col_4": "dd",
                "col_5": "e",
                "col_6": "ff",
                "col_7": "g",
            },
            {
                "unique_id": 4,
                "col_1": "aa",
                "col_2": "bb",
                "col_3": "cc",
                "col_4": "d",
                "col_5": "ee",
                "col_6": "ff",
                "col_7": "gg",
            },
        ]
    )
    A, B, C, D, E, F, G = (
        "col_1_l = col_1_r",
        "col_2_l = col_2_r",
        "col_3_l = col_3_r",
        "col_4_l = col_4_r",
        "col_5_l = col_5_r",
        "col_6_l = col_6_r",
        "col_7_l = col_7_r",
    )

    complex_condition_sql = " OR ".join(
        [
            f"({A} AND {B})",
            f"NOT ({C} AND {D})",
            f"({B} AND NOT {E})",
            f"({F} AND (NOT {A} AND {G}))",
        ]
    )
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "output_column_name": "my_comparison",
                "comparison_levels": [
                    cll.null_level("col_1"),
                    cll.exact_match_level("col_7"),
                    cll.exact_match_level("col_3"),
                    {
                        "sql_condition": complex_condition_sql,
                        "label_for_charts": "complex condition",
                    },
                    cll.else_level(),
                ],
            }
        ],
    }
    linker = DuckDBLinker(df, settings)

    linker.estimate_parameters_using_expectation_maximisation("1=1")


def test_normalise():
    # check that the sqlglot normaliser is doing what we think
    # try to not impose specific form too strongly, so we aren't too tightly
    # coupled to the implementationß
    sql_syntax_tree = parse_one("a or (b and c)")
    sql_cnf = normalize(sql_syntax_tree).sql().lower()

    subclauses_expected = [
        ["a or c", "c or a"],
        ["a or b", "b or a"],
    ]

    # get subclauses and remove outer parens
    subclauses_found = map(lambda s: s.strip("()"), sql_cnf.split(" and "))

    # loop through subclauses, make sure that we have exactly one of each
    for found in subclauses_found:
        term_found = False
        for i, expected in enumerate(subclauses_expected):
            if found in expected:
                del subclauses_expected[i]
                term_found = True
                break
        assert term_found, f"CNF contains unexpected clause '{found}'"
    assert not subclauses_expected

    # and a slightly more complex statement
    sql_syntax_tree = parse_one("(a and b) or (a and c) or (c and d) or (d and b)")
    sql_cnf = normalize(sql_syntax_tree).sql().lower()

    subclauses_expected = [
        ["b or c", "c or b"],
        ["a or d", "d or a"],
    ]

    subclauses_found = map(lambda s: s.strip("()"), sql_cnf.split(" and "))

    # loop through subclauses, make sure that we have exactly one of each
    for found in subclauses_found:
        term_found = False
        for i, expected in enumerate(subclauses_expected):
            if found in expected:
                del subclauses_expected[i]
                term_found = True
                break
        assert term_found, f"CNF contains unexpected clause '{found}'"
    assert not subclauses_expected

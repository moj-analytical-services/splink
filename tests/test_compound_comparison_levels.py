import pandas as pd

from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import splink.duckdb.duckdb_comparison_level_library as cll


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

    sql_two_out_of_three_match = (
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
                        "sql_condition": sql_two_out_of_three_match,
                        "label_for_charts": "2/3 match",
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

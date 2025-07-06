from __future__ import annotations

import duckdb
import pandas as pd
import pytest
from sqlglot import parse_one

import splink.comparison_level_library as cll
from splink.internals.blocking_rule_library import block_on
from splink.internals.comparison_library import (
    ArrayIntersectAtSizes,
    CustomComparison,
    ExactMatch,
    JaroWinklerAtThresholds,
)
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.reusable_function_detection import _find_repeated_functions
from tests.decorator import mark_with_dialects_excluding, mark_with_dialects_including


def create_test_data(
    con: duckdb.DuckDBPyConnection | None = None, return_as_ddb_table=False
):
    con = con or duckdb.connect(":memory:")
    data = [
        {
            "unique_id": 1,
            "first_name": "John",
            "surname": "Smith",
            "name_tokens": ["john", "j", "johnny"],
            "name_tokens_with_freq": [
                {"token": "john", "rel_freq": 0.001},
                {"token": "j", "rel_freq": 0.01},
            ],
            "postcode": "AB12 3CD",
        },
        {
            "unique_id": 2,
            "first_name": "Johnny",
            "surname": "Smith",
            "name_tokens": ["johnny", "john", "j"],
            "name_tokens_with_freq": [
                {"token": "johnny", "rel_freq": 0.002},
                {"token": "j", "rel_freq": 0.01},
            ],
            "postcode": "AB12 3CD",
        },
        {
            "unique_id": 3,
            "first_name": "Jon",
            "surname": "Smith",
            "name_tokens": ["jon", "j"],
            "name_tokens_with_freq": [
                {"token": "jon", "rel_freq": 0.003},
                {"token": "j", "rel_freq": 0.01},
            ],
            "postcode": "AB12 3CD",
        },
        {
            "unique_id": 4,
            "first_name": "Jon",
            "surname": "Smyth",
            "name_tokens": ["jane", "j"],
            "name_tokens_with_freq": [
                {"token": "jon", "rel_freq": 0.004},
                {"token": "j", "rel_freq": 0.01},
            ],
            "postcode": "AB12 3CD",
        },
    ]

    # Impose explicit schema
    con.execute("""
        CREATE TABLE df (
            unique_id INTEGER,
            first_name VARCHAR,
            surname VARCHAR,
            name_tokens VARCHAR[],
            name_tokens_with_freq STRUCT(token VARCHAR, rel_freq FLOAT)[],
            postcode VARCHAR
        )
    """)

    con.executemany(
        """
        INSERT INTO df
        (unique_id, first_name, surname, name_tokens, name_tokens_with_freq, postcode)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        [
            (
                d["unique_id"],
                d["first_name"],
                d["surname"],
                d["name_tokens"],
                d["name_tokens_with_freq"],
                d["postcode"],
            )
            for d in data
        ],
    )

    if return_as_ddb_table:
        return con.table("df")
    else:
        return pd.DataFrame(data)


def calculate_tf_product_array_sql(token_rel_freq_array_name):
    sql = f"""
    list_reduce(
        list_prepend(
            1.0::FLOAT,
            list_transform(
                list_intersect(
                    {token_rel_freq_array_name}_l,
                    {token_rel_freq_array_name}_r
                ),
                x -> x.rel_freq::float
            )
        ),
        (p, q) -> p * q
    ) *
    list_reduce(
        list_prepend(
            1.0,
            list_transform(
                list_concat(
                    array_filter(
                        {token_rel_freq_array_name}_l,
                        y -> NOT array_contains(
                            list_transform(
                                {token_rel_freq_array_name}_r, x -> x.token
                            ),
                            y.token
                        )
                    ),
                    array_filter(
                        {token_rel_freq_array_name}_r,
                        y -> NOT array_contains(
                            list_transform(
                                {token_rel_freq_array_name}_l, x -> x.token
                            ),
                            y.token
                        )
                    )
                ),
                x -> x.rel_freq
            )
        ),
        (p, q) -> p / q^0.33
    )
    """
    return sql


@mark_with_dialects_including("duckdb")
def test_optimisation_with_custom_comparison():
    """Test that the SQL optimization works with complex term frequency comparisons"""
    # Set up DuckDB specific test data
    con = duckdb.connect(":memory:")
    df = create_test_data(con, return_as_ddb_table=True)

    # Complex comparison with term frequencies - DuckDB specific syntax
    custom_rel_freq_comparison = {
        "output_column_name": "name_tokens_with_freq",
        "comparison_levels": [
            cll.NullLevel("name_tokens_with_freq"),
            {
                "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-12
                """,
                "label_for_charts": "Array product is less than 1e-12",
            },
            {
                "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-8
                """,
                "label_for_charts": "Array product is less than 1e-8",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
    }

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            JaroWinklerAtThresholds("first_name", [0.9, 0.8, 0.7]),
            ExactMatch("surname"),
            custom_rel_freq_comparison,
        ],
        "blocking_rules_to_generate_predictions": [block_on("surname")],
    }

    db_api_1 = DuckDBAPI(connection=con)
    linker_no_opt = Linker(df, settings, db_api=db_api_1)
    df_no_opt_splink = linker_no_opt.inference.predict(
        experimental_function_reuse_optimisation=False
    )

    df_no_opt = df_no_opt_splink.as_pandas_dataframe()

    # Check the optimisation was not applied
    assert (
        "reusable_function_values_optimisation"
        not in df_no_opt_splink.sql_used_to_create
    )

    # Run with optimization
    db_api_2 = DuckDBAPI(connection=con)
    linker_opt = Linker(df, settings, db_api=db_api_2)
    df_opt_splink = linker_opt.inference.predict(
        experimental_function_reuse_optimisation=True
    )
    df_opt = df_opt_splink.as_pandas_dataframe()

    assert (
        pytest.approx(df_no_opt["match_weight"].sum()) == df_opt["match_weight"].sum()
    )

    # Check the optimisation was applied
    assert "reusable_function_values_optimisation" in df_opt_splink.sql_used_to_create


@mark_with_dialects_excluding("postgres", "sqlite")
def test_optimisation_with_em_training(test_helpers, dialect):
    """Test that the SQL optimization works correctly with EM training"""
    # This works because estimate u is not random on a small dataset because
    # it creates all possible pairs
    helper = test_helpers[dialect]
    df = create_test_data()

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            JaroWinklerAtThresholds("first_name", [0.9, 0.8, 0.7]),
            CustomComparison(
                output_column_name="surname",
                comparison_levels=[
                    cll.NullLevel("surname"),
                    cll.ExactMatchLevel("surname"),
                    cll.JaroWinklerLevel("surname", distance_threshold=0.98),
                    cll.LevenshteinLevel("surname", distance_threshold=1),
                    cll.JaroWinklerLevel("surname", distance_threshold=0.9),
                    cll.LevenshteinLevel("surname", distance_threshold=2),
                ],
            ),
        ],
        "blocking_rules_to_generate_predictions": [block_on("surname")],
    }

    linker_no_opt = Linker(df, settings, **helper.extra_linker_args())
    linker_no_opt.training.estimate_u_using_random_sampling(
        max_pairs=1e3, experimental_function_reuse_optimisation=False
    )
    linker_no_opt.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"), experimental_function_reuse_optimisation=False
    )
    df_no_opt = linker_no_opt.inference.predict(
        experimental_function_reuse_optimisation=False
    ).as_pandas_dataframe()

    linker_opt = Linker(df, settings, **helper.extra_linker_args())
    linker_opt.training.estimate_u_using_random_sampling(
        max_pairs=1e3, experimental_function_reuse_optimisation=True
    )
    linker_opt.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"), experimental_function_reuse_optimisation=True
    )
    df_opt = linker_opt.inference.predict(
        experimental_function_reuse_optimisation=True
    ).as_pandas_dataframe()

    assert (
        pytest.approx(df_no_opt["match_weight"].sum()) == df_opt["match_weight"].sum()
    )


def test_find_repeated_functions_nested():
    """Test that _find_repeated_functions only returns outermost repeated functions"""

    sql = """CASE
    WHEN fn1(a, fn2(b, c)) > 0 THEN 1
    WHEN fn1(a, fn2(b, c)) < 5 THEN 2
    ELSE 0
    END"""

    repeated_functions, _ = _find_repeated_functions([sql], "duckdb")

    # We should find exactly one reusable function: fn1(a, fn2(b, c))
    # even though fn2(b, c) is repeated, but nested inside it
    assert len(repeated_functions) == 1

    found_ast = parse_one(repeated_functions[0]["function_sql"], read="duckdb")
    expected_ast = parse_one("fn1(a, fn2(b, c))", read="duckdb")
    assert found_ast == expected_ast

    # Check the nested function fn2(b, c) is not included in the results
    nested_asts = {
        parse_one(f["function_sql"], read="duckdb") for f in repeated_functions
    }
    fn2_ast = parse_one("fn2(b, c)", read="duckdb")
    assert fn2_ast not in nested_asts

    # Example 2:  Extremely complex SQL with nested functions

    expected_fn = calculate_tf_product_array_sql("name_tokens_with_freq")
    expected_ast = parse_one(expected_fn, read="duckdb")

    # Check its robust to SQL formatting differences i.e. same
    # semantics different formatting.
    expected_fn_fmt_diff = expected_fn.replace("\n", "").replace(" ", "  ")

    # The full SQL with the repeated function
    sql = f"""CASE
    WHEN {expected_fn} < 1e-12 then 2
    WHEN {expected_fn_fmt_diff} < 1e-14 then 1
    ELSE 0
    END"""

    repeated_functions, _ = _find_repeated_functions([sql], "duckdb")

    # Should find exactly one repeated function
    assert len(repeated_functions) == 1

    # Parse the found function and compare with expected
    found_ast = parse_one(repeated_functions[0]["function_sql"], read="duckdb")
    assert found_ast.sql("duckdb") == expected_ast.sql("duckdb")


def test_find_repeated_functions_multiple():
    expected_fns = ["fn1(a, b)", "fn2(c, d)"]
    expected_asts = [parse_one(fn, read="duckdb") for fn in expected_fns]

    sql = """CASE
    WHEN fn1(a, b) > 0.9 AND fn2(c, d) < 3 THEN 3
    WHEN fn1(a, b) > 0.8 AND fn2(c, d) < 4 THEN 2
    ELSE 0
    END"""

    repeated_functions, _ = _find_repeated_functions([sql], "duckdb")

    # Should find exactly two repeated functions
    assert len(repeated_functions) == 2

    # Compare found functions with expected
    found_asts = [
        parse_one(func["function_sql"], read="duckdb") for func in repeated_functions
    ]
    found_sqls = {ast.sql("duckdb") for ast in found_asts}
    expected_sqls = {ast.sql("duckdb") for ast in expected_asts}
    assert found_sqls == expected_sqls


def test_find_repeated_functions_single_use():
    """Test that _find_repeated_functions ignores non-repeated functions"""

    sql = """CASE
    WHEN fn1(a, b) > 0.9 AND fn2(c, d) < 3 THEN 3
    WHEN fn3(a, b) < 2 THEN 2
    ELSE 0
    END"""

    repeated_functions, _ = _find_repeated_functions([sql], "duckdb")
    assert len(repeated_functions) == 0


def test_find_repeated_functions_with_different_args():
    """Test that functions with diff arguments are treated as diff functions"""

    expected_fns = ["fn(a, b)", "fn(c, d)"]
    expected_asts = [parse_one(fn, read="duckdb") for fn in expected_fns]

    sql = """CASE
    WHEN fn(a, b) > 5 AND fn(c, d) > 10 THEN 3
    WHEN fn(a, b) > 3 AND fn(c, d) > 8 THEN 2
    ELSE 0
    END"""

    repeated_functions, _ = _find_repeated_functions([sql], "duckdb")

    # Compare found functions with expected
    found_asts = [
        parse_one(func["function_sql"], read="duckdb") for func in repeated_functions
    ]
    found_sqls = {ast.sql("duckdb") for ast in found_asts}
    expected_sqls = {ast.sql("duckdb") for ast in expected_asts}
    assert found_sqls == expected_sqls


@mark_with_dialects_including("duckdb")
def test_optimisation_with_multiple_complex_comparisons():
    """Test matches the result when running the model pre-optimisation
    i.e. on master before the optimisation PR was merged
    """
    # Set up DuckDB specific test data
    con = duckdb.connect(":memory:")
    df = create_test_data(con, return_as_ddb_table=True)

    # Define complex comparisons
    custom_tf_comparison = {
        "output_column_name": "name_tokens_with_freq",
        "comparison_levels": [
            cll.NullLevel("name_tokens_with_freq"),
            {
                "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-12
                """,
                "label_for_charts": "Array product is less than 1e-12",
            },
            {
                "sql_condition": f"""
                    {calculate_tf_product_array_sql("name_tokens_with_freq")} < 1e-8
                """,
                "label_for_charts": "Array product is less than 1e-8",
            },
            {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
        ],
    }

    surname_comparison = CustomComparison(
        output_column_name="surname",
        comparison_levels=[
            cll.NullLevel("surname"),
            cll.ExactMatchLevel("surname"),
            cll.JaroWinklerLevel("surname", distance_threshold=0.98),
            cll.LevenshteinLevel("surname", distance_threshold=1),
            cll.JaroWinklerLevel("surname", distance_threshold=0.9),
            cll.LevenshteinLevel("surname", distance_threshold=2),
        ],
    )

    custom_postcode_comparison = CustomComparison(
        output_column_name="postcode",
        comparison_levels=[
            cll.NullLevel("postcode"),
            {
                "sql_condition": """
            LEVENSHTEIN(substr(postcode_l,1,3),substr(postcode_r,1,3)) <= 1
            AND
            JARO_WINKLER_SIMILARITY(postcode_l, postcode_r) >= 0.9
            """,
                "label_for_charts": "lev and jaro 1",
            },
            {
                "sql_condition": """
            LEVENSHTEIN(substr(postcode_l,1,3),substr(postcode_r,1,4)) <= 2
            AND
            JARO_WINKLER_SIMILARITY(postcode_l, postcode_r) >= 0.8
            """,
                "label_for_charts": "lev and jaro 2",
            },
        ],
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            JaroWinklerAtThresholds("first_name", [0.9, 0.8, 0.7]),
            surname_comparison,
            custom_tf_comparison,
            ArrayIntersectAtSizes("name_tokens", [3, 2, 1]),
            custom_postcode_comparison,
        ],
        "blocking_rules_to_generate_predictions": [block_on("surname")],
        "max_iterations": 2,
    }

    linker = Linker(df, settings, db_api=DuckDBAPI(connection=con))

    linker.training.estimate_u_using_random_sampling(
        max_pairs=10000, experimental_function_reuse_optimisation=True
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"), experimental_function_reuse_optimisation=True
    )

    predictions_df = linker.inference.predict(
        experimental_function_reuse_optimisation=True
    ).as_pandas_dataframe()

    # This was calculated on master branch prior to the optimisation
    # so this is a check that the new implementation gives exactly the same result
    # Note estimate u is deterministic because it creates all pairs
    assert pytest.approx(-64.830722) == predictions_df["match_weight"].sum()

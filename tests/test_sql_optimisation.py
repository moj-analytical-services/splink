import duckdb
import pytest

from splink.internals.blocking_rule_library import block_on
from splink.internals.comparison_library import (
    ArrayIntersectAtSizes,
    ExactMatch,
    JaroWinklerAtThresholds,
    LevenshteinAtThresholds,
)
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
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
        },
    ]

    # Impose explicit schema
    con.execute("""
        CREATE TABLE df (
            unique_id INTEGER,
            first_name VARCHAR,
            surname VARCHAR,
            name_tokens VARCHAR[],
            name_tokens_with_freq STRUCT(token VARCHAR, rel_freq FLOAT)[]
        )
    """)

    con.executemany(
        """
        INSERT INTO df (unique_id, first_name, surname, name_tokens, name_tokens_with_freq)
        VALUES (?, ?, ?, ?, ?)
    """,
        [
            (
                d["unique_id"],
                d["first_name"],
                d["surname"],
                d["name_tokens"],
                d["name_tokens_with_freq"],
            )
            for d in data
        ],
    )

    if return_as_ddb_table:
        return con.table("df")
    else:
        return con.table("df").df()


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
            {
                "sql_condition": '"name_tokens_with_freq_l" IS NULL OR "name_tokens_with_freq_r" IS NULL',
                "label_for_charts": "name_tokens_with_freq is NULL",
                "is_null_level": True,
            },
            {
                "sql_condition": """
                    list_reduce(list_prepend(1.0, list_transform(list_intersect(name_tokens_with_freq_l, name_tokens_with_freq_r), x -> CAST(x.rel_freq AS FLOAT))), (p, q) -> p * q) < 1e-12
                """,
                "label_for_charts": "Array product is less than 1e-12",
            },
            {
                "sql_condition": """
                    list_reduce(list_prepend(1.0, list_transform(list_intersect(name_tokens_with_freq_l, name_tokens_with_freq_r), x -> CAST(x.rel_freq AS FLOAT))), (p, q) -> p * q) < 1e-8
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
    df_no_opt = linker_no_opt.inference.predict(
        experimental_optimisation=False
    ).as_pandas_dataframe()

    # Run with optimization
    db_api_2 = DuckDBAPI(connection=con)
    linker_opt = Linker(df, settings, db_api=db_api_2)
    df_opt = linker_opt.inference.predict(
        experimental_optimisation=True
    ).as_pandas_dataframe()

    assert (
        pytest.approx(df_no_opt["match_weight"].sum()) == df_opt["match_weight"].sum()
    )


@mark_with_dialects_excluding()
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
            ArrayIntersectAtSizes("name_tokens", [3, 2, 1]),
            LevenshteinAtThresholds("surname", [0.9, 0.8, 0.7]),
        ],
        "blocking_rules_to_generate_predictions": [block_on("surname")],
    }

    linker_no_opt = Linker(df, settings, **helper.extra_linker_args())
    linker_no_opt.training.estimate_u_using_random_sampling(
        max_pairs=1e3, experimental_optimisation=False
    )
    linker_no_opt.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"), experimental_optimisation=False
    )
    df_no_opt = linker_no_opt.inference.predict(
        experimental_optimisation=False
    ).as_pandas_dataframe()

    linker_opt = Linker(df, settings, **helper.extra_linker_args())
    linker_opt.training.estimate_u_using_random_sampling(
        max_pairs=1e3, experimental_optimisation=True
    )
    linker_opt.training.estimate_parameters_using_expectation_maximisation(
        block_on("surname"), experimental_optimisation=True
    )
    df_opt = linker_opt.inference.predict(
        experimental_optimisation=True
    ).as_pandas_dataframe()

    assert (
        pytest.approx(df_no_opt["match_weight"].sum()) == df_opt["match_weight"].sum()
    )

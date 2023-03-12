import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_level_library as clld
import splink.duckdb.duckdb_comparison_library as cld
import splink.spark.spark_comparison_level_library as clls
import splink.spark.spark_comparison_library as cls
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


@pytest.mark.parametrize(
    ("cl", "cll", "Linker"),
    [
        pytest.param(cld, clld, DuckDBLinker, id="DuckDB Datediff Integration Tests"),
        pytest.param(cls, clls, SparkLinker, id="Spark Datediff Integration Tests"),
    ],
)
def test_datediff_levels(spark, cl, cll, Linker):

    # Capture differing comparison levels to allow unique settings generation
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "Tom",
                "dob": "2000-01-01",
            },
            {
                "unique_id": 2,
                "first_name": "Robin",
                "dob": "2000-01-30",
            },
            {
                "unique_id": 3,
                "first_name": "Zoe",
                "dob": "1995-09-30",
            },
            {
                "unique_id": 4,
                "first_name": "Sam",
                "dob": "1966-07-30",
            },
            {
                "unique_id": 5,
                "first_name": "Andy",
                "dob": "1996-03-25",
            },
            {
                "unique_id": 6,
                "first_name": "Alice",
                "dob": "2000-03-25",
            },
            {
                "unique_id": 7,
                "first_name": "Afua",
                "dob": "1960-01-01",
            },
        ]
    )

    exact_match_fn = cl.exact_match("first_name")

    # For testing the cll version
    dob_diff = {
        "output_column_name": "dob",
        "comparison_levels": [
            cll.null_level("dob"),
            cll.exact_match_level("dob"),
            cll.datediff_level(
                date_col="dob",
                date_threshold=30,
                date_metric="day",
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=12,
                date_metric="month",
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=5,
                date_metric="year",
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=100,
                date_metric="year",
            ),
            cll.else_level(),
        ],
    }

    settings_cll = {
        "link_type": "dedupe_only",
        "comparisons": [exact_match_fn, dob_diff],
    }

    settings_cl = {
        "link_type": "dedupe_only",
        "comparisons": [
            exact_match_fn,
            cl.datediff_at_thresholds(
                "dob", [30, 12, 5, 100], ["day", "month", "year", "year"]
            ),
        ],
    }

    # We need to put our column in datetime format for this to work
    df["dob"] = pd.to_datetime(df["dob"])

    if Linker == SparkLinker:
        df = spark.createDataFrame(df)
        df.persist()
    linker = Linker(df, settings_cl)
    cl_df_e = linker.predict().as_pandas_dataframe()
    linker = Linker(df, settings_cll)
    cll_df_e = linker.predict().as_pandas_dataframe()

    linker_outputs = {
        "cl": cl_df_e,
        "cll": cll_df_e,
    }

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {1: 11, 2: 6, 3: 3, 4: 1}

    # Check gamma sizes are as expected
    for gamma, gamma_lookup in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            print(type(linker_pred["gamma_dob"]))
            assert sum(linker_pred["gamma_dob"] == gamma) == gamma_lookup

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    gamma_lookup = {
        4: [(1, 2)],
        3: [(3, 5), (1, 6), (2, 6)],
        2: [(1, 3), (2, 3), (1, 5), (2, 5), (3, 6), (5, 6)],
    }

    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            for linker_name, linker_pred in linker_outputs.items():

                print(f"Checking IDs: {left}, {right} for {linker_name}")

                assert (
                    linker_pred.loc[
                        (linker_pred.unique_id_l == left)
                        & (linker_pred.unique_id_r == right)
                    ]["gamma_dob"].values[0]
                    == gamma
                )


@pytest.mark.parametrize(
    ("cl"),
    [
        pytest.param(cld, id="DuckDB Datediff Error Checks"),
        pytest.param(cls, id="Spark Datediff Error Checks"),
    ],
)
def test_datediff_error_logger(cl):

    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        cl.datediff_at_thresholds("dob", [1], ["day", "month", "year", "year"])
    # Negative threshold
    with pytest.raises(ValueError):
        cl.datediff_at_thresholds("dob", [-1], ["day"])
    # Invalid metric
    with pytest.raises(ValueError):
        cl.datediff_at_thresholds("dob", [1], ["dy"])
    # Threshold len == 0
    with pytest.raises(ValueError):
        cl.datediff_at_thresholds("dob", [], ["dy"])
    # Metric len == 0
    with pytest.raises(ValueError):
        cl.datediff_at_thresholds("dob", [1], [])

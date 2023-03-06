import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_level_library as clld
import splink.duckdb.duckdb_comparison_library as cld
import splink.spark.spark_comparison_level_library as clls
import splink.spark.spark_comparison_library as cls
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker

# Capture differing comparison levels to allow unique settings generation
comp_levels = {
    "duckdb": {"cl": cld, "cll": clld},
    "spark": {"cl": cls, "cll": clls},
}


def gen_settings(type, comparison_library=False):
    c_levels = comp_levels[type]
    exact_match_fn = c_levels["cl"].exact_match("first_name")

    # For testing the comparison library version
    if comparison_library:
        return {
            "link_type": "dedupe_only",
            "comparisons": [
                exact_match_fn,
                c_levels["cl"].datediff_at_thresholds(
                    "dob", [30, 12, 5, 100], ["day", "month", "year", "year"]
                ),
            ],
        }

    # For testing the cll version
    dob_diff = {
        "output_column_name": "dob",
        "comparison_levels": [
            c_levels["cll"].null_level("dob"),
            c_levels["cll"].exact_match_level("dob"),
            c_levels["cll"].datediff_level(
                date_col="dob",
                date_threshold=30,
                date_metric="day",
            ),
            c_levels["cll"].datediff_level(
                date_col="dob",
                date_threshold=12,
                date_metric="month",
            ),
            c_levels["cll"].datediff_level(
                date_col="dob",
                date_threshold=5,
                date_metric="year",
            ),
            c_levels["cll"].datediff_level(
                date_col="dob",
                date_threshold=100,
                date_metric="year",
            ),
            c_levels["cll"].else_level(),
        ],
    }

    return {
        "link_type": "dedupe_only",
        "comparisons": [exact_match_fn, dob_diff],
    }


def test_datediff_levels(spark):
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

    # Generate our various settings objs
    duck_settings = gen_settings("duckdb")
    duck_settings_cll = gen_settings("duckdb", True)

    spark_settings = gen_settings("spark")
    spark_settings_cll = gen_settings("spark", True)

    # We need to put our column in datetime format for this to work
    df["dob"] = pd.to_datetime(df["dob"])

    # DuckDBFrame
    dlinker = DuckDBLinker(df, duck_settings)
    duck_df_e = dlinker.predict().as_pandas_dataframe()
    dlinker = DuckDBLinker(df, duck_settings_cll)
    duck_cl_df_e = dlinker.predict().as_pandas_dataframe()

    # SparkFrame
    sparkdf = spark.createDataFrame(df)
    sparkdf.persist()

    splinker = SparkLinker(
        sparkdf,
        spark_settings,
    )
    sp_df_e = splinker.predict().as_pandas_dataframe()
    splinker = SparkLinker(sparkdf, spark_settings_cll)
    sp_cl_df_e = splinker.predict().as_pandas_dataframe()

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {1: 11, 2: 6, 3: 3, 4: 1}

    linker_outputs = {
        "duckdb_cll": duck_df_e,
        "duckdb_cl": duck_cl_df_e,
        "spark_cll": sp_df_e,
        "spark_cl": sp_cl_df_e,
    }

    # Check gamma sizes are as expected
    for k, v in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            assert sum(linker_pred["gamma_dob"] == k) == v

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        4: {"id_pairs": [(1, 2)]},
        3: {"id_pairs": [(3, 5), (1, 6), (2, 6)]},
        2: {"id_pairs": [(1, 3), (2, 3), (1, 5), (2, 5), (3, 6), (5, 6)]},
    }

    for k, v in size_gamma_lookup.items():
        for ids in v["id_pairs"]:
            for linker_name, linker_pred in linker_outputs.items():
                print(f"Checking IDs: {ids} for {linker_name}")

                assert (
                    linker_pred.loc[
                        (linker_pred.unique_id_l == ids[0])
                        & (linker_pred.unique_id_r == ids[1])
                    ]["gamma_dob"].values[0]
                    == k
                )


def test_datediff_error_logger():
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        cld.datediff_at_thresholds("dob", [1], ["day", "month", "year", "year"])
    # Negative threshold
    with pytest.raises(ValueError):
        cld.datediff_at_thresholds("dob", [-1], ["day"])
    # Invalid metric
    with pytest.raises(ValueError):
        cld.datediff_at_thresholds("dob", [1], ["dy"])
    # Threshold len == 0
    with pytest.raises(ValueError):
        cld.datediff_at_thresholds("dob", [], ["dy"])
    # Metric len == 0
    with pytest.raises(ValueError):
        cld.datediff_at_thresholds("dob", [1], [])

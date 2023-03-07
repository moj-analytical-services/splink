import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_library as cld
import splink.duckdb.duckdb_comparison_template_library as ctld
import splink.spark.spark_comparison_library as cls
import splink.spark.spark_comparison_template_library as ctls

from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


def test_duckdb_date_comparison_run():
    ctld.date_comparison("date")


def test_spark_date_comparison_run():
    ctls.date_comparison("date")


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
                "dob": "2000-01-01",
            },
        ]
    )

    # Generate our various settings objs
    duck_settings = {
        "link_type": "dedupe_only",
        "comparisons": [ctld.date_comparison("dob")],
    }

    spark_settings = {
        "link_type": "dedupe_only",
        "comparisons": [ctls.date_comparison("dob")],
    }

    # We need to put our column in datetime format for this to work
    df["dob"] = pd.to_datetime(df["dob"])

    # DuckDBFrame
    dlinker = DuckDBLinker(df, duck_settings)
    duck_df_e = dlinker.predict().as_pandas_dataframe()

    # SparkFrame
    sparkdf = spark.createDataFrame(df)
    sparkdf.persist()

    splinker = SparkLinker(
        sparkdf,
        spark_settings,
    )
    sp_df_e = splinker.predict().as_pandas_dataframe()

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {0: 17, 1: 3, 2: 0, 3: 1}

    linker_outputs = {
        "dlinker": duck_df_e  # ,
        # "slinker": sp_df_e,
    }

    # Check gamma sizes are as expected
    for k, v in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            print(f"k={k} and v={v}")

            assert sum(linker_pred["gamma_dob"] == k) == v

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        3: {"id_pairs": [(1, 2)]},
        1: {"id_pairs": [(3, 5), (2, 6), (2, 6)]},
        0: {"id_pairs": [(1, 3), (2, 3), (1, 5), (2, 5), (3, 6), (5, 6)]},
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


def test_date_comparison_error_logger():
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        ctld.date_comparison("date", datediff_thresholds=[[1, 2], ["month"]])
    """ # Check metric and threshold are the correct way around
    with pytest.raises(ValueError):
        ctld.date_comparison("date",
                             datediff_thresholds=[['month'], [1]]
        ) """
    # Invalid metric
    with pytest.raises(ValueError):
        ctld.date_comparison("date", datediff_thresholds=[[1], ["dy"]])
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctld.date_comparison("date", datediff_thresholds=[[], ["day"]])
    # Metric len == 0
    with pytest.raises(ValueError):
        ctld.date_comparison("date", datediff_thresholds=[[1], []])

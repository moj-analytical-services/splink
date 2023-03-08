import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_template_library as ctld
import splink.spark.spark_comparison_template_library as ctls
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


@pytest.mark.parametrize(
    ("ctl"),
    [
        pytest.param(ctld, id="DuckDB Date Comparison Simple Run Test"),
        pytest.param(ctls, id="Spark Date Comparison Simple Run Test"),
    ],
)
def test_date_comparison_run(ctl):
    ctl.date_comparison("date")


@pytest.mark.parametrize(
    ("ctl", "Linker"),
    [
        pytest.param(ctld, DuckDBLinker, id="DuckDB Date Comparison Integration Tests"),
        pytest.param(ctls, SparkLinker, id="Spark Date Comparison Integration Tests"),
    ],
)
def test_datediff_levels(spark, ctl, Linker):

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
                "dob": "2000-01-20",
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
                "first_name": "Andy's Twin",
                "dob": "1996-03-25",
            },
            {
                "unique_id": 7,
                "first_name": "Alice",
                "dob": "1999-12-28",
            },
            {
                "unique_id": 8,
                "first_name": "Afua",
                "dob": "2000-01-01",
            },
            {
                "unique_id": 9,
                "first_name": "Ross",
                "dob": "2000-10-20",
            },
        ]
    )

    # Generate our various settings objs
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [ctl.date_comparison("dob")],
    }

    # We need to put our column in datetime format for this to work
    df["dob"] = pd.to_datetime(df["dob"])

    if Linker == SparkLinker:
        df = spark.createDataFrame(df)
        df.persist()
    linker = Linker(df, settings)
    linker_output = linker.predict().as_pandas_dataframe()

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {0: 23, 1: 5, 2: 3, 3: 3, 4: 1, 5: 1}

    # Check gamma sizes are as expected
    for gamma, gamma_lookup in size_gamma_lookup.items():
        print(f"gamma={gamma} and gamma_lookup={gamma_lookup}")

        assert sum(linker_output["gamma_dob"] == gamma) == gamma_lookup

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        5: [[1, 8]],
        4: [(5, 6)],
        3: [(2, 9)],
        2: [(7, 8)],
        1: [(3, 5), (1, 9)],
        0: [(1, 3), (2, 3), (1, 5), (2, 5), (4, 7)],
    }

    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            print(f"Checking IDs: {left}, {right}")

            assert (
                linker_output.loc[
                    (linker_output.unique_id_l == left)
                    & (linker_output.unique_id_r == right)
                ]["gamma_dob"].values[0]
                == gamma
            )


@pytest.mark.parametrize(
    ("ctl"),
    [
        pytest.param(ctld, id="DuckDB Datediff Error Checks"),
        pytest.param(ctls, id="Spark Datediff Error Checks"),
    ],
)
def test_date_comparison_error_logger(ctl):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        ctl.date_comparison(
            "date", datediff_thresholds=[1, 2], datediff_metrics=["month"]
        )
    """ # Check metric and threshold are the correct way around
    with pytest.raises(ValueError):
        ctld.date_comparison("date",
                             datediff_thresholds=[['month'], [1]]
        ) """
    # Invalid metric
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[1], datediff_metrics=["dy"])
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[], datediff_metrics=["day"])
    # Metric len == 0
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[1], datediff_metrics=[])

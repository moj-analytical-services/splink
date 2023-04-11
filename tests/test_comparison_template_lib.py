import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_template_library as ctld
import splink.spark.spark_comparison_template_library as ctls
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


## date_comparison
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
    ("ctl"),
    [
        pytest.param(ctld, id="DuckDB Date Comparison Jaro Test"),
        pytest.param(ctls, id="Spark Date Comparison Jaro Test"),
    ],
)
def test_date_comparison_jaro_run(ctl):
    ctl.date_comparison("date", levenshtein_thresholds=[], jaro_thresholds=[0.9])

@pytest.mark.parametrize(
    ("ctl"),
    [
        pytest.param(ctld, id="DuckDB Date Comparison Jaro-Winkler Test"),
        pytest.param(ctls, id="Spark Date Comparison Jaro-Winkler Test"),
    ],
)
def test_date_comparison_jw_run(ctl):
    ctl.date_comparison("date", levenshtein_thresholds=[], jaro_winkler_thresholds=[0.9])


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

    # # Dict key: {gamma_level value: size}
    size_gamma_lookup = {0: 8, 1: 15, 2: 8, 3: 3, 4: 0, 5: 2}

    # Check gamma sizes are as expected
    for gamma, expected_size in size_gamma_lookup.items():
        print(f"gamma={gamma} and gamma_lookup={expected_size}")

        assert sum(linker_output["gamma_dob"] == gamma) == expected_size

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        5: [[1, 8]],
        3: [(2, 9)],
        2: [(7, 8), (1, 9)],
        1: [(3, 7)],
        0: [(1, 4)],
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
    # Check metric and threshold are the correct way around
    with pytest.raises(ValueError):
        ctld.date_comparison(
            "date", datediff_thresholds=["month"], datediff_metrics=[1]
        )
    # Invalid metric
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[1], datediff_metrics=["dy"])
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[], datediff_metrics=["day"])
    # Metric len == 0
    with pytest.raises(ValueError):
        ctl.date_comparison("date", datediff_thresholds=[1], datediff_metrics=[])


## name_comparison


@pytest.mark.parametrize(
    ("ctl"),
    [
        pytest.param(ctld, id="DuckDB Name Comparison Simple Run Test"),
        pytest.param(ctls, id="Spark Name Comparison Simple Run Test"),
    ],
)
def test_name_comparison_run(ctl):
    ctl.name_comparison("first_name")


@pytest.mark.parametrize(
    ("ctl", "Linker"),
    [
        pytest.param(ctld, DuckDBLinker, id="DuckDB Name Comparison Integration Tests"),
        pytest.param(ctls, SparkLinker, id="Spark Name Comparison Integration Tests"),
    ],
)
def test_name_comparison_levels(spark, ctl, Linker):

    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "Robert",
                "first_name_metaphone": "RBRT",
                "dob": "1996-03-25",
            },
            {
                "unique_id": 2,
                "first_name": "Rob",
                "first_name_metaphone": "RB",
                "dob": "1996-03-25",
            },
            {
                "unique_id": 3,
                "first_name": "Robbie",
                "first_name_metaphone": "RB",
                "dob": "1999-12-28",
            },
            {
                "unique_id": 4,
                "first_name": "Bobert",
                "first_name_metaphone": "BB",
                "dob": "2000-01-01",
            },
            {
                "unique_id": 5,
                "first_name": "Bobby",
                "first_name_metaphone": "BB",
                "dob": "2000-10-20",
            },
            {
                "unique_id": 6,
                "first_name": "Robert",
                "first_name_metaphone": "RBRT",
                "dob": "1996-03-25",
            },
        ]
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            ctl.name_comparison(
                "first_name",
                phonetic_col_name="first_name_metaphone",
            )
        ],
    }

    if Linker == SparkLinker:
        df = spark.createDataFrame(df)
        df.persist()
    linker = Linker(df, settings)
    linker_output = linker.predict().as_pandas_dataframe()

    # # Dict key: {gamma_level value: size}
    size_gamma_lookup = {0: 8, 1: 4, 2: 0, 3: 2, 4: 1}
    # 4: exact_match
    # 3: dmetaphone exact match
    # 2: jaro_winkler > 0.95
    # 1: jaro_winkler > 0.8
    # 0: else

    # Check gamma sizes are as expected
    for gamma, expected_size in size_gamma_lookup.items():
        print(f"gamma={gamma} and gamma_lookup={expected_size}")

        assert (
            sum(linker_output["gamma_custom_first_name_first_name_metaphone"] == gamma)
            == expected_size
        )

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        4: [[1, 6]],
        3: [(2, 3), (4, 5)],
        2: [],
        1: [(1, 2), (4, 6)],
        0: [(2, 4), (5, 6)],
    }

    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            print(f"Checking IDs: {left}, {right}")

            assert (
                linker_output.loc[
                    (linker_output.unique_id_l == left)
                    & (linker_output.unique_id_r == right)
                ]["gamma_custom_first_name_first_name_metaphone"].values[0]
                == gamma
            )

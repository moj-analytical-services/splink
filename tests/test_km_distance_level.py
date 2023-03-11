import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_level_library as clld
import splink.duckdb.duckdb_comparison_library as cld
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


# from splink.athena.athena_linker import AthenaLinker


@pytest.mark.parametrize(
    ("cl"),
    [
        pytest.param(cld, id="DuckDB Datediff Integration Tests"),
        #        pytest.param(cls, SparkLinker, id="Spark Datediff Integration Tests"),
    ],
)
def test_simple_run(cl):

    print(
        cl.distance_in_km_at_thresholds(
            lat_col="lat", long_col="long", km_thresholds=[1, 5, 10]
        ).as_dict()
    )


@pytest.mark.parametrize(
    ("cl", "cll", "Linker"),
    [
        pytest.param(
            cld, clld, DuckDBLinker, id="DuckDB Distance in KM Integration Tests"
        ),
        #        pytest.param(cls, clls, SparkLinker, id="Spark Distance in KM Integration Tests"),
        #        pytest.param(cls, clls, AthenaLinker, id="Athena Distance in KM Integration Tests"),
    ],
)
def test_km_distance_levels(cl, cll, Linker):
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "name": "102 Petty France",
                "lat": 51.500516,
                "long": -0.133192,
            },
            {
                "unique_id": 2,
                "name": "10 South Colonnade",
                "lat": 51.504444,
                "long": -0.021389,
            },
            {
                "unique_id": 3,
                "name": "Houses of Parliament",
                "lat": 51.499479,
                "long": -0.124809,
            },
            {
                "unique_id": 4,
                "name": "5 Wellington Place",
                "lat": 53.796105,
                "long": -1.549725,
            },
            {
                "unique_id": 5,
                "name": "102 Petty France Duplicate",
                "lat": 51.500516,
                "long": -0.133192,
            },
            {
                "unique_id": 6,
                "name": "Splink",
                "lat": 53.3338,
                "long": -6.24488,
            },
        ]
    )

    settings_cl = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.distance_in_km_at_thresholds(
                lat_col="lat", long_col="long", km_thresholds=[0, 1, 10, 300]
            )
        ],
    }

    # For testing the cll version
    {
        "output_column_name": "dob",
        "comparison_levels": [
            {
                "sql_condition": "(lat_l IS NULL OR lat_r IS NULL) OR (long_l IS NULL OR long_r IS NULL)",
                "label_for_charts": "Null",
                "is_null_level": True,
            },
            cll.distance_in_km_level(lat_col="lat", long_col="long", km_threshold=0),
            cll.distance_in_km_level(lat_col="lat", long_col="long", km_threshold=1),
            cll.distance_in_km_level(
                lat_col="lat",
                long_col="long",
                km_threshold=20,
            ),
            cll.distance_in_km_level(
                lat_col="lat",
                long_col="long",
                km_threshold=100,
            ),
            cll.else_level(),
        ],
    }


    if Linker == SparkLinker:
        df = spark.createDataFrame(df)
        df.persist()
    linker = Linker(df, settings_cl)
    cl_df_e = linker.predict().as_pandas_dataframe()
    linker = Linker(df, settings_cl)
    linker.predict().as_pandas_dataframe()

    linker_outputs = {
        "cl": cl_df_e,
        # "cll": cll_df_e,
    }

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {0: 5, 1: 4, 2: 3, 3: 2, 4: 1}

    # Check gamma sizes are as expected
    for gamma, gamma_lookup in size_gamma_lookup.items():
        print(linker_outputs)
        # linker_pred = linker_outputs
        for linker_pred in linker_outputs.values():
            # lat_long_colname =
            gamma_column_name_options = [
                "gamma_custom_long_lat",
                "gamma_custom_lat_long",
            ]  # lat and long switch unpredictably
            gamma_column_name = linker_pred.columns[
                linker_pred.columns.str.contains("|".join(gamma_column_name_options))
            ]
            print("<<<<<<<<<<<<<<<LOOK HERE>>>>>>>>>>>>>>>>>>>")
            print(linker_pred[gamma_column_name].value_counts())
            assert (
                sum(linker_pred[gamma_column_name].squeeze() == gamma) == gamma_lookup
            )

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    gamma_lookup = {
        3: [(1, 2)],
        1: [(3, 2)],
        2: [(1, 3)],
    }


""" 
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
 """

""" def test_datediff_levels(spark, cl, cll, Linker):

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

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [exact_match_fn, dob_diff],
    }

    settings_cll = {
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
    linker = Linker(df, settings)
    df_e = linker.predict().as_pandas_dataframe()
    linker = Linker(df, settings_cll)
    cl_df_e = linker.predict().as_pandas_dataframe()

    # # Dict key: {size: gamma_level value}
    size_gamma_lookup = {1: 11, 2: 6, 3: 3, 4: 1}

    linker_outputs = {
        "cll": df_e,
        "cl": cl_df_e,
    }

    # Check gamma sizes are as expected
    for gamma, gamma_lookup in size_gamma_lookup.items():
        for linker_pred in linker_outputs.values():
            assert sum(linker_pred["gamma_dob"] == gamma) == gamma_lookup

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
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
                ) """


""" @pytest.mark.parametrize(
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
        cl.datediff_at_thresholds("dob", [1], []) """

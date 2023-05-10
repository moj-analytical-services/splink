import logging

import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_level_library as clld
import splink.duckdb.duckdb_comparison_library as cld
import splink.spark.spark_comparison_level_library as clls
import splink.spark.spark_comparison_library as cls
from splink import exceptions
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

    for gamma, id_pairs in gamma_lookup.items():
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


@pytest.mark.parametrize(
    ("cl", "cll", "Linker", "date_format"),
    [
        pytest.param(cld, clld, DuckDBLinker, "%Y-%m-%d", id="DuckDB Datediff Integration Tests"),
        pytest.param(cls, clls, SparkLinker, "yyyy-mm-dd", id="Spark Datediff Integration Tests"),
    ],
)
def test_datediff_with_str_casting(spark, cl, cll, Linker, date_format, caplog):
    caplog.set_level(logging.INFO)

    def simple_dob_linker(spark, df, dobs=[], 
                          date_format_param=None, 
                          invalid_dates_as_null=False,
                          Linker=Linker
                          ):
        settings_cl = {
            "link_type": "dedupe_only",
            "comparisons": [
                cl.exact_match("first_name"),
                cl.datediff_at_thresholds(
                    "dob",
                    [30, 12, 5, 100],
                    ["day", "month", "year", "year"],
                    cast_strings_to_date=True,
                    date_format=date_format_param,
                    invalid_dates_as_null=invalid_dates_as_null
                ),
            ],
        }
        # For testing the cll version
        if invalid_dates_as_null:
            null_level_regex=date_format_param
        else:
            null_level_regex=None

        dob_diff = {
            "output_column_name": "dob",
            "comparison_levels": [
                cll.null_level("dob", valid_string_regex=null_level_regex),
                cll.exact_match_level("dob"),
                cll.datediff_level(
                    date_col="dob",
                    date_threshold=30,
                    date_metric="day",
                    cast_strings_to_date=True,
                    date_format=date_format_param,
                ),
                cll.datediff_level(
                    date_col="dob",
                    date_threshold=12,
                    date_metric="month",
                    cast_strings_to_date=True,
                    date_format=date_format_param,
                ),
                cll.datediff_level(
                    date_col="dob",
                    date_threshold=5,
                    date_metric="year",
                    cast_strings_to_date=True,
                    date_format=date_format_param,
                ),
                cll.datediff_level(
                    date_col="dob",
                    date_threshold=100,
                    date_metric="year",
                    cast_strings_to_date=True,
                    date_format=date_format_param,
                ),
                cll.else_level(),
            ],
        }

        settings = {
            "link_type": "dedupe_only",
            "comparisons": [cl.exact_match("first_name"), dob_diff],
        }
        if len(dobs) == df.shape[0]:
            df["dob"] = dobs

        if Linker == SparkLinker:
            df = spark.createDataFrame(df)
            df.persist()

        linker = Linker(df, settings)
        df_e1 = linker.predict().as_pandas_dataframe()

        linker = Linker(df, settings_cl)
        df_e2 = linker.predict().as_pandas_dataframe()
        return df_e2

    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "Tom",
                "dob": "02-03-1993",
            },
            {
                "unique_id": 2,
                "first_name": "Robin",
                "dob": "30-01-1992",
            },
        ]
    )

    if Linker == SparkLinker:
        expected_bad_dates_error = exceptions.SplinkException
        valid_date_formats = ["d/M/y", "d-M-y", "M/d/y", "y/M/d", "y-M-d"]
    elif Linker == DuckDBLinker:
        expected_bad_dates_error = exceptions.SplinkException
        valid_date_formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%Y-%m-%d",
        ]

    # first test some dates which should work
    simple_dob_linker(
        spark,
        df,
        dobs=["03/04/1994", "19/02/1993"],
        date_format_param=valid_date_formats[0],
        Linker=Linker,
    )
    simple_dob_linker(
        spark,
        df,
        dobs=["03-04-1994", "19-02-1993"],
        date_format_param=valid_date_formats[1],
        Linker=Linker,
    )
    simple_dob_linker(
        spark,
        df,
        dobs=["04/05/1994", "10/02/1993"],
        date_format_param=valid_date_formats[2],
        Linker=Linker,
    )
    simple_dob_linker(
        spark,
        df,
        dobs=["1994/05/04", "1993/05/02"],
        date_format_param=valid_date_formats[3],
        Linker=Linker,
    )
    simple_dob_linker(
        spark,
        df,
        dobs=["1994-05-04", "1993-05-02"],
        date_format_param=valid_date_formats[4],
        Linker=Linker,
    )
    # test the default date formats
    simple_dob_linker(
        spark,
        df,
        dobs=["1994-05-04", "1993-05-02"],
        date_format_param=None,
        Linker=Linker,
    )

    # now test some bad dates with DuckDB which you
    # expect to throw error. Don't run these tests with
    # Spark as does not throw error
    # in response to badly formatted dates
    if Linker == DuckDBLinker:
        # Then test some bad dates
        # bad date type 1:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                spark,
                df,
                dobs=["1994-05-04", "1993-14-02"],
                date_format_param=None,
                Linker=Linker,
            )

        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                spark,
                df,
                dobs=["1994/05/04", "1993/14/02"],
                date_format_param=valid_date_formats[3],
                Linker=Linker,
            )

        # bad date type 2:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                spark,
                df,
                dobs=["03-14-1994", "19-22-1993"],
                date_format_param=valid_date_formats[1],
                Linker=Linker,
            )

        # mis-match between date formats:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                spark,
                df,
                dobs=["03-14-1994", "19/22/1993"],
                date_format_param=valid_date_formats[1],
                Linker=Linker,
            )

        # mis-match between input dates and expected date format
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                spark,
                df,
                dobs=["20-04-1993", "19-02-1993"],
                date_format_param=valid_date_formats[3],
                Linker=Linker,
            )

    # Test some incorrectly formatted dates with DuckDB which you
    # expect to throw error, but with the invalid_dates_as_null
    # parameter switched on there is no error.
    # Don't run these tests with
    # Spark as does not throw error
    # in response to badly formatted dates


    if Linker == DuckDBLinker:
        # mis-match between date formats:
        simple_dob_linker(
                spark,
                df,
                dobs=["03-14-1994", "19/22/1993"],
                date_format_param=valid_date_formats[1],
                invalid_dates_as_null=True,
                Linker=Linker,
            )

        # mis-match between input dates and expected date format
        simple_dob_linker(
                spark,
                df,
                dobs=["20-04-1993", "19-02-1993"],
                date_format_param=valid_date_formats[3],
                invalid_dates_as_null=True,
                Linker=Linker,
            )

import logging

import pandas as pd
import pytest

from splink import exceptions

from .decorator import mark_with_dialects_excluding


# No SQLite - no array comparisons in library
@mark_with_dialects_excluding("sqlite")
def test_datediff_levels(test_helpers, dialect):
    helper = test_helpers[dialect]
    cl = helper.cl
    cll = helper.cll

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
    df = helper.convert_frame(df)

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
                cast_strings_to_date=True,
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=12,
                date_metric="month",
                cast_strings_to_date=True,
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=5,
                date_metric="year",
                cast_strings_to_date=True,
            ),
            cll.datediff_level(
                date_col="dob",
                date_threshold=100,
                date_metric="year",
                cast_strings_to_date=True,
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
                "dob",
                [30, 12, 5, 100],
                ["day", "month", "year", "year"],
                cast_strings_to_date=True,
            ),
        ],
    }

    # We need to put our column in datetime format for this to work
    linker = helper.Linker(df, settings_cl, **helper.extra_linker_args())
    cl_df_e = linker.predict().as_pandas_dataframe()
    linker = helper.Linker(df, settings_cll, **helper.extra_linker_args())
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
            assert (
                linker_pred.loc[
                    (linker_pred.unique_id_l == left)
                    & (linker_pred.unique_id_r == right)
                ]["gamma_dob"].values[0]
                == gamma
            )


@mark_with_dialects_excluding("sqlite")
def test_datediff_error_logger(test_helpers, dialect):
    helper = test_helpers[dialect]
    cl = helper.cl
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


@mark_with_dialects_excluding("sqlite", "postgres")
def test_datediff_with_str_casting(test_helpers, dialect, caplog):
    caplog.set_level(logging.INFO)
    helper = test_helpers[dialect]
    cl = helper.cl
    cll = helper.cll

    def simple_dob_linker(
        df,
        dobs=[],
        date_format_param=None,
        invalid_dates_as_null=False,
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
                    invalid_dates_as_null=invalid_dates_as_null,
                ),
            ],
        }
        # For testing the cll version
        if invalid_dates_as_null:
            null_level_regex = date_format_param
        else:
            null_level_regex = None

        dob_diff = {
            "output_column_name": "dob",
            "comparison_levels": [
                cll.null_level(
                    "dob",
                    valid_string_pattern=null_level_regex,
                    invalid_dates_as_null=invalid_dates_as_null,
                ),
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

        df = helper.convert_frame(df)
        linker = helper.Linker(df, settings, **helper.extra_linker_args())
        df_e1 = linker.predict().as_pandas_dataframe()

        linker = helper.Linker(df, settings_cl, **helper.extra_linker_args())
        df_e2 = linker.predict().as_pandas_dataframe()
        return df_e1, df_e2

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

    if dialect in ("spark", "postgres"):
        expected_bad_dates_error = exceptions.SplinkException
        valid_date_formats = ["d/M/y", "d-M-y", "M/d/y", "y/M/d", "y-M-d"]
    elif dialect == "duckdb":
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
        df,
        dobs=["03/04/1994", "19/02/1993"],
        date_format_param=valid_date_formats[0],
    )
    simple_dob_linker(
        df,
        dobs=["03-04-1994", "19-02-1993"],
        date_format_param=valid_date_formats[1],
    )
    simple_dob_linker(
        df,
        dobs=["04/05/1994", "10/02/1993"],
        date_format_param=valid_date_formats[2],
    )
    simple_dob_linker(
        df,
        dobs=["1994/05/04", "1993/05/02"],
        date_format_param=valid_date_formats[3],
    )
    simple_dob_linker(
        df,
        dobs=["1994-05-04", "1993-05-02"],
        date_format_param=valid_date_formats[4],
    )
    # test the default date formats
    simple_dob_linker(
        df,
        dobs=["1994-05-04", "1993-05-02"],
        date_format_param=None,
    )

    # now test some bad dates with DuckDB which you
    # expect to throw error. Don't run these tests with
    # Spark as does not throw error
    # in response to badly formatted dates
    if dialect == "duckdb":
        # Then test some bad dates
        # bad date type 1:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                df,
                dobs=["1994-05-04", "1993-14-02"],
                date_format_param=None,
            )

        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                df,
                dobs=["1994/05/04", "1993/14/02"],
                date_format_param=valid_date_formats[3],
            )

        # bad date type 2:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                df,
                dobs=["03-14-1994", "19-22-1993"],
                date_format_param=valid_date_formats[1],
            )

        # mis-match between date formats:
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                df,
                dobs=["03-14-1994", "19/22/1993"],
                date_format_param=valid_date_formats[1],
            )

        # mis-match between input dates and expected date format
        with pytest.raises(expected_bad_dates_error):
            simple_dob_linker(
                df,
                dobs=["20-04-1993", "19-02-1993"],
                date_format_param=valid_date_formats[3],
            )

    # Test some incorrectly formatted dates with the
    # invalid_dates_as_null parameter
    # invalid date (month > 12)
    simple_dob_linker(
        df,
        dobs=["03-14-1994", "19-12-1993"],
        date_format_param=valid_date_formats[1],
        invalid_dates_as_null=True,
    )

    # mis-match between input dates and expected date format
    simple_dob_linker(
        df,
        dobs=["20/04/1993", "19-02-1993"],
        date_format_param=valid_date_formats[3],
        invalid_dates_as_null=True,
    )

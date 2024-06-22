import pandas as pd
import pytest

import splink.internals.comparison_library as cl
from splink.internals.column_expression import ColumnExpression
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import (
    ComparisonTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_email_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]
    test_spec = ComparisonTestSpec(
        cl.EmailComparison("email"),
        tests=[
            LiteralTestValues(
                {"email_l": "john@smith.com", "email_r": "john@smith.com"},
                expected_gamma_val=4,
            ),
            LiteralTestValues(
                {"email_l": "rebecca@company.com", "email_r": "rebecca@smith.com"},
                expected_gamma_val=3,
            ),
            LiteralTestValues(
                {"email_l": "rebecca@company.com", "email_r": "rebbecca@company.com"},
                expected_gamma_val=2,
            ),
            LiteralTestValues(
                {"email_l": "rebecca@company.com", "email_r": "rebbecca@xyz.com"},
                expected_gamma_val=1,
            ),
            LiteralTestValues(
                {"email_l": "john@smith.com", "email_r": "rebbecca@xyz.com"},
                expected_gamma_val=0,
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("sqlite")
def test_date_of_birth_comparison_levels(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]
    test_spec = ComparisonTestSpec(
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            separate_1st_january=True,
            invalid_dates_as_null=True,
        ),
        tests=[
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-01", "date_of_birth_r": "1990-01-01"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"date_of_birth_l": "2012-01-01", "date_of_birth_r": "2012-02-02"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1985-03-11", "date_of_birth_r": "1985-01-01"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-20", "date_of_birth_r": "1990-05-20"},
                expected_gamma_val=5,  # Exact match
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-01", "date_of_birth_r": "1990-05-11"},
                expected_gamma_val=4,  # Damerau-Levenshtein distance <= 1
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-20", "date_of_birth_r": "1990-06-19"},
                expected_gamma_val=3,  # Date difference <= 1 month
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-20", "date_of_birth_r": "1991-04-21"},
                expected_gamma_val=2,  # Date difference <= 1 year
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-20", "date_of_birth_r": "1999-02-20"},
                expected_gamma_val=1,  # Date difference <= 10 years
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-05-20", "date_of_birth_r": "2010-01-17"},
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.DateOfBirthComparison(
            ColumnExpression("dob").try_parse_date(),
            input_is_string=False,
            separate_1st_january=True,
        ),
        tests=[
            LiteralTestValues(
                {"dob_l": "1990-01-01", "dob_r": "1990-01-01"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"dob_l": "2012-01-01", "dob_r": "2012-02-02"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"dob_l": "1985-03-11", "dob_r": "1985-01-01"},
                expected_gamma_val=6,  # Exact match on year (1st of January only)
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-20", "dob_r": "1990-05-20"},
                expected_gamma_val=5,  # Exact match
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-01", "dob_r": "1990-05-11"},
                expected_gamma_val=4,  # Damerau-Levenshtein distance <= 1
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-20", "dob_r": "1990-06-19"},
                expected_gamma_val=3,  # Date difference <= 1 month
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-20", "dob_r": "1991-04-21"},
                expected_gamma_val=2,  # Date difference <= 1 year
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-20", "dob_r": "1999-02-20"},
                expected_gamma_val=1,  # Date difference <= 10 years
            ),
            LiteralTestValues(
                {"dob_l": "1990-05-20", "dob_r": "2010-01-17"},
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            invalid_dates_as_null=False,
        ),
        tests=[
            LiteralTestValues(
                {"date_of_birth_l": "2012-02-02", "date_of_birth_r": "2012-02-02"},
                expected_gamma_val=5,  # Exact match
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1985-31-11", "date_of_birth_r": "1985-01-11"},
                expected_gamma_val=4,  # Damerau-Levenshtein distance <= 1
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1985-31-11", "date_of_birth_r": "1985-01-22"},
                expected_gamma_val=0,  # Everythign else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.DateOfBirthComparison(
            "date_of_birth",
            input_is_string=True,
            separate_1st_january=False,
            datetime_thresholds=[1, 2, 5],
            datetime_metrics=["day", "month", "year"],
        ),
        tests=[
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-01", "date_of_birth_r": "1990-01-01"},
                expected_gamma_val=5,  # Exact match
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1989-12-30", "date_of_birth_r": "1989-12-31"},
                expected_gamma_val=4,  # Damerau-Levenshtein distance <= 1
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-31", "date_of_birth_r": "1990-02-01"},
                expected_gamma_val=3,  # Date difference <= 1 day
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-01", "date_of_birth_r": "1990-02-15"},
                expected_gamma_val=2,  # Date difference <= 2 months
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-01", "date_of_birth_r": "1994-07-30"},
                expected_gamma_val=1,  # Date difference <= 5 years
            ),
            LiteralTestValues(
                {"date_of_birth_l": "1990-01-01", "date_of_birth_r": "2000-11-23"},
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_date_comparison_error_logger(dialect):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        cl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[1, 2],
            datetime_metrics=["month"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Check metric and threshold are the correct way around
    with pytest.raises(TypeError):
        cl.DateOfBirthComparison(
            "date",
            datetime_thresholds=["month"],
            datetime_metrics=[1],
            input_is_string=True,
        ).get_comparison(dialect)
    # Invalid metric
    with pytest.raises(ValueError):
        cl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[1],
            datetime_metrics=["dy"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Threshold len == 0
    with pytest.raises(ValueError):
        cl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[],
            datetime_metrics=["day"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Metric len == 0
    with pytest.raises(ValueError):
        cl.DateOfBirthComparison(
            "date", datetime_thresholds=[1], datetime_metrics=[], input_is_string=True
        ).get_comparison(dialect)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_postcode_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        cl.PostcodeComparison("postcode"),
        tests=[
            LiteralTestValues(
                {"postcode_l": "SW1A 1AA", "postcode_r": "SW1A 1AA"},
                expected_gamma_val=4,  # Exact match on full postcode
            ),
            LiteralTestValues(
                {"postcode_l": "SW1A 1AA", "postcode_r": "SW1A 1AB"},
                expected_gamma_val=3,  # Exact match on sector
            ),
            LiteralTestValues(
                {"postcode_l": "SW1A 1AA", "postcode_r": "SW1A 2AA"},
                expected_gamma_val=2,  # Exact match on district
            ),
            LiteralTestValues(
                {"postcode_l": "SW1A 1AA", "postcode_r": "SW2A 1AA"},
                expected_gamma_val=1,  # Exact match on area
            ),
            LiteralTestValues(
                {"postcode_l": "SW1A 1AA", "postcode_r": "NW1A 1AA"},
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.PostcodeComparison(
            "postcode",
            lat_col="latitude",
            long_col="longitude",
            km_thresholds=[1, 5, 10],
        ),
        tests=[
            LiteralTestValues(
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 1AA",
                    "latitude_l": 51.501009,
                    "longitude_l": -0.141588,
                    "latitude_r": 51.501009,
                    "longitude_r": -0.141588,
                },
                expected_gamma_val=5,  # Exact match on full postcode
            ),
            LiteralTestValues(
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 1AB",
                    "latitude_l": 51.501009,
                    "longitude_l": -0.141588,
                    "latitude_r": 51.501009,
                    "longitude_r": -0.141599,
                },
                expected_gamma_val=4,  # sector
            ),
            LiteralTestValues(
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 2AA",
                    "latitude_l": 51.501009,
                    "longitude_l": -0.141588,
                    "latitude_r": 51.502009,
                    "longitude_r": -0.142588,
                },
                expected_gamma_val=3,  # Within 1 km threshold
            ),
            LiteralTestValues(
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "NW1A 1AA",
                    "latitude_l": 51.501009,
                    "longitude_l": -0.141588,
                    "latitude_r": 51.5155,
                    "longitude_r": -0.1752,
                },
                expected_gamma_val=2,  # Within 10 km threshold
            ),
            LiteralTestValues(
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "M1 1AA",
                    "latitude_l": 51.501009,
                    "longitude_l": -0.141588,
                    "latitude_r": 53.478,
                    "longitude_r": -2.242631,
                },
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_name_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]
    test_spec = ComparisonTestSpec(
        cl.NameComparison("name"),
        tests=[
            LiteralTestValues(
                {"name_l": "John", "name_r": "John"},
                expected_gamma_val=4,  # Exact match
            ),
            LiteralTestValues(
                {"name_l": "Stephen", "name_r": "Stephan"},
                expected_gamma_val=3,  # Jaro-Winkler similarity > 0.92
            ),
            LiteralTestValues(
                {"name_l": "Stephen", "name_r": "Steven"},
                expected_gamma_val=2,  # Jaro-Winkler similarity > 0.88
            ),
            LiteralTestValues(
                {"name_l": "Stephen", "name_r": "Steve"},
                expected_gamma_val=1,  # Jaro-Winkler similarity > 0.70
            ),
            LiteralTestValues(
                {"name_l": "Alice", "name_r": "Bob"},
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.NameComparison("name", dmeta_col_name="dmeta_name"),
        tests=[
            LiteralTestValues(
                {
                    "name_l": "Smith",
                    "name_r": "Smith",
                    "dmeta_name_l": ["SM0", "XMT"],
                    "dmeta_name_r": ["SM0", "XMT"],
                },
                expected_gamma_val=5,  # Exact match
            ),
            LiteralTestValues(
                {
                    "name_l": "Stephen",
                    "name_r": "Stephan",
                    "dmeta_name_l": ["STFN"],
                    "dmeta_name_r": ["STFN"],
                },
                expected_gamma_val=4,  # Jaro-Winkler similarity > 0.92
            ),
            LiteralTestValues(
                {
                    "name_l": "Stephen",
                    "name_r": "Steven",
                    "dmeta_name_l": ["STFN"],
                    "dmeta_name_r": ["STFN"],
                },
                expected_gamma_val=3,  # Jaro-Winkler similarity > 0.88
            ),
            LiteralTestValues(
                {
                    "name_l": "Smith",
                    "name_r": "Schmidt",
                    "dmeta_name_l": ["SM0", "XMT"],
                    "dmeta_name_r": ["SMT", "XMT"],
                },
                expected_gamma_val=2,  # Array intersect > 1
            ),
            LiteralTestValues(
                {
                    "name_l": "Stephen",
                    "name_r": "Steve",
                    "dmeta_name_l": ["STFN"],
                    "dmeta_name_r": ["STF"],
                },
                expected_gamma_val=1,  # Jaro-Winkler similarity > 0.70
            ),
            LiteralTestValues(
                {
                    "name_l": "Alice",
                    "name_r": "Bob",
                    "dmeta_name_l": ["ALS"],
                    "dmeta_name_r": ["PP"],
                },
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_forename_surname_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        cl.ForenameSurnameComparison("forename", "surname"),
        tests=[
            LiteralTestValues(
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                },
                expected_gamma_val=6,  # Exact match on forename and surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "James",
                    "forename_r": "Smith",
                    "surname_l": "Smith",
                    "surname_r": "James",
                },
                expected_gamma_val=5,  # Exact match on forename and surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "Stephen",
                    "forename_r": "Stephan",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                },
                expected_gamma_val=4,  # jwsim > 0.92 on fname, exact match on surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "Stephen",
                    "forename_r": "Steven",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                },
                expected_gamma_val=3,  # jwsim > 0.88 on fname, exact match on surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Doe",
                    "surname_r": "Smith",
                },
                expected_gamma_val=1,  # Exact match forename, anything else on surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "Alice",
                    "forename_r": "Bob",
                    "surname_l": "Jones",
                    "surname_r": "Smith",
                },
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

    test_spec = ComparisonTestSpec(
        cl.ForenameSurnameComparison(
            "forename",
            "surname",
            forename_surname_concat_col_name="forename_surname_concat",
        ),
        tests=[
            LiteralTestValues(
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "forename_surname_concat_l": "John Smith",
                    "forename_surname_concat_r": "John Smith",
                },
                expected_gamma_val=6,  # Exact match on forename and surname
            ),
            LiteralTestValues(
                {
                    "forename_l": "Alice",
                    "forename_r": "Bob",
                    "surname_l": "Jones",
                    "surname_r": "Smith",
                    "forename_surname_concat_l": "Alice Jones",
                    "forename_surname_concat_r": "Bob Smith",
                },
                expected_gamma_val=0,  # Anything else
            ),
        ],
    )
    run_tests_with_args(test_spec, db_api)

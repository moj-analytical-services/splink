import pandas as pd
import pytest

import splink.internals.comparison_library as cl
import splink.internals.comparison_template_library as ctl
from splink.internals.column_expression import ColumnExpression
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import (
    ComparisonTestSpec,
    LiteralTestValues,
    run_tests_with_args,
)

## name_comparison


@mark_with_dialects_excluding("postgres", "sqlite")
def test_name_comparison_run(dialect):
    ctl.NameComparison("first_name").get_comparison(dialect)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_name_comparison_levels(dialect, test_helpers):
    helper = test_helpers[dialect]
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
            ctl.NameComparison(
                "first_name",
                phonetic_col_name="first_name_metaphone",
            )
        ],
    }

    df = helper.convert_frame(df)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_output = linker.inference.predict().as_pandas_dataframe()

    # # Dict key: {gamma_level value: size}
    size_gamma_lookup = {0: 6, 1: 6, 2: 0, 3: 2, 4: 1}
    # 4: exact_match
    # 3: dmetaphone exact match
    # 2: jaro_winkler > 0.9
    # 1: jaro_winkler > 0.8
    # 0: else

    # Check gamma sizes are as expected
    for gamma, expected_size in size_gamma_lookup.items():
        assert sum(linker_output["gamma_first_name"] == gamma) == expected_size

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        4: [[1, 6]],
        3: [(2, 3), (4, 5)],
        2: [],
        1: [(1, 2), (2, 6), (4, 6)],
        0: [(2, 4), (5, 6)],
    }

    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            assert (
                linker_output.loc[
                    (linker_output.unique_id_l == left)
                    & (linker_output.unique_id_r == right)
                ]["gamma_first_name"].values[0]
                == gamma
            )


@mark_with_dialects_excluding("postgres", "sqlite")
def test_forename_surname_comparison_run(dialect):
    ctl.ForenameSurnameComparison("first_name", "surname").get_comparison(dialect)


## forename_surname_comparison


@mark_with_dialects_excluding("postgres", "sqlite")
def test_forename_surname_comparison_levels(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "forename": "Robert",
                "surname": "Smith",
            },
            {
                "unique_id": 2,
                "forename": "Robert",
                "surname": "Smith",
            },
            {
                "unique_id": 3,
                "forename": "Smith",
                "surname": "Robert",
            },
            {
                "unique_id": 4,
                "forename": "Bobert",
                "surname": "Franks",
            },
            {
                "unique_id": 5,
                "forename": "Bobby",
                "surname": "Smith",
            },
            {
                "unique_id": 6,
                "forename": "Robert",
                "surname": "Jones",
            },
            {
                "unique_id": 7,
                "forename": "James",
                "surname": "Smyth",
            },
        ]
    )

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [ctl.ForenameSurnameComparison("forename", "surname")],
    }

    df = helper.convert_frame(df)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_output = linker.inference.predict().as_pandas_dataframe()

    # # Dict key: {gamma_level value: size}
    size_gamma_lookup = {0: 8, 1: 3, 2: 3, 3: 2, 4: 2, 5: 2, 6: 1}
    # 6: exact_match
    # 5: reversed_cols
    # 4: surname match
    # 3: forename match
    # 2: surname jaro_winkler > 0.88
    # 1: forename jaro_winkler > 0.88
    # 0: else

    # Check gamma sizes are as expected
    for gamma, expected_size in size_gamma_lookup.items():
        gamma_matches = linker_output.filter(like="gamma_forename_surname") == gamma
        gamma_matches_size = gamma_matches.sum().values[0]
        assert gamma_matches_size == expected_size

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        6: [(1, 2)],
        5: [(2, 3)],
        4: [(2, 5)],
        3: [(1, 6)],
        2: [(5, 7)],
        1: [(1, 4), (4, 6)],
        0: [(3, 4), (6, 7)],
    }
    for gamma, id_pairs in size_gamma_lookup.items():
        for left, right in id_pairs:
            assert (
                linker_output.loc[
                    (linker_output.unique_id_l == left)
                    & (linker_output.unique_id_r == right)
                ]
                .filter(like="gamma_forename_surname")
                .values[0][0]
                == gamma
            )


# PostcodeComparison


@mark_with_dialects_excluding("postgres", "sqlite")
def test_postcode_comparison_levels(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    col_name = "postcode"

    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "first_name": "Andy",
                "postcode": "SE1P 0NY",
                "lat": 53.95,
                "long": -1.08,
            },
            {
                "unique_id": 2,
                "first_name": "Andy's twin",
                "postcode": "SE1P 0NY",
                "lat": 53.95,
                "long": -1.08,
            },
            {
                "unique_id": 3,
                "first_name": "Tom",
                "postcode": "SE1P 0PZ",
                "lat": 53.95,
                "long": -1.08,
            },
            {
                "unique_id": 4,
                "first_name": "Robin",
                "postcode": "SE1P 4UY",
                "lat": 53.95,
                "long": -1.08,
            },
            {
                "unique_id": 5,
                "first_name": "Sam",
                "postcode": "SE2 7TR",
                "lat": 53.95,
                "long": -1.08,
            },
            {
                "unique_id": 6,
                "first_name": "Zoe",
                "postcode": "sw15 8uy",
                "lat": 53.95,
                "long": -1.08,
            },
        ]
    )

    # Generate our various settings objs
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            ctl.PostcodeComparison(
                col_name=col_name,
                lat_col="lat",
                long_col="long",
                km_thresholds=5,
            )
        ],
    }

    df = helper.convert_frame(df)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_output = linker.inference.predict().as_pandas_dataframe()

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_level: tuple of ID pairs}
    size_gamma_lookup = {
        5: [(1, 2)],
        4: [(1, 3), (2, 3)],
        3: [(1, 4), (2, 4), (3, 4)],
        2: [(1, 5), (2, 5), (3, 5), (4, 5)],
        1: [(1, 6), (2, 6), (3, 6), (4, 6), (5, 6)],
    }

    test_gamma_assert(linker_output, size_gamma_lookup, col_name)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_email_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]
    test_spec = ComparisonTestSpec(
        ctl.EmailComparison("email"),
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
        ctl.DateOfBirthComparison(
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
        ctl.DateOfBirthComparison(
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
        ctl.DateOfBirthComparison(
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
        ctl.DateOfBirthComparison(
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
        ctl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[1, 2],
            datetime_metrics=["month"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Check metric and threshold are the correct way around
    with pytest.raises(TypeError):
        ctl.DateOfBirthComparison(
            "date",
            datetime_thresholds=["month"],
            datetime_metrics=[1],
            input_is_string=True,
        ).get_comparison(dialect)
    # Invalid metric
    with pytest.raises(ValueError):
        ctl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[1],
            datetime_metrics=["dy"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctl.DateOfBirthComparison(
            "date",
            datetime_thresholds=[],
            datetime_metrics=["day"],
            input_is_string=True,
        ).get_comparison(dialect)
    # Metric len == 0
    with pytest.raises(ValueError):
        ctl.DateOfBirthComparison(
            "date", datetime_thresholds=[1], datetime_metrics=[], input_is_string=True
        ).get_comparison(dialect)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_postcode_comparison(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["database_api"]

    test_spec = ComparisonTestSpec(
        ctl.PostcodeComparison("postcode"),
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
        ctl.PostcodeComparison(
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
        ctl.NameComparison("name"),
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
        ctl.NameComparison("name", dmeta_col_name="dmeta_name"),
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
        ctl.ForenameSurnameComparison("forename", "surname"),
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
        ctl.ForenameSurnameComparison(
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

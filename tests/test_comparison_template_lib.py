import pandas as pd
import pytest

import splink.comparison_template_library as ctl
from splink.column_expression import ColumnExpression

from .decorator import mark_with_dialects_excluding

# TODO: we can probably restore limited postgres (+ maybe sqlite?) testing,
# if we use the right option
## DateComparison


@mark_with_dialects_excluding("postgres", "sqlite")
def test_date_comparison_run(dialect):
    ctl.DateComparison("date").get_comparison(dialect)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_date_comparison_dl_run(dialect):
    ctl.DateComparison(
        "date", fuzzy_thresholds=[1], fuzzy_metric="levenshtein"
    ).get_comparison(dialect)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_datediff_levels(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    col_name = "dob"

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
        "comparisons": [
            ctl.DateComparison(
                # TODO: revert to default damerau_levenshtein metric
                ColumnExpression("dob").try_parse_date(),
                fuzzy_metric="levenshtein",
                fuzzy_thresholds=[2],
            )
        ],
    }

    # We need to put our column in datetime format for this to work

    df = helper.convert_frame(df)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker.debug_mode = True
    linker_output = linker.predict().as_pandas_dataframe()

    # # Dict key: {gamma_level value: size}
    # 0 - else, 1 - 10 years, 2 - 1 year, 3 - 1 month, 4 - fuzzy, 5 - date match
    size_gamma_lookup = {0: 8, 1: 15, 2: 5, 3: 3, 4: 3, 5: 2}
    # Dam-lev version - difference between 1-months and fuzzy level
    # size_gamma_lookup = {0: 8, 1: 15, 2: 6, 3: 5, 4: 1, 5: 2}

    # Check gamma sizes are as expected
    for gamma, expected_size in size_gamma_lookup.items():
        assert sum(linker_output["gamma_dob"] == gamma) == expected_size

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        5: [[1, 8]],
        4: [(2, 9)],
        3: [(7, 8)],
        2: [(1, 9)],
        1: [(3, 7)],
        0: [(1, 4)],
    }

    test_gamma_assert(linker_output, size_gamma_lookup, col_name)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_date_comparison_error_logger(dialect):
    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1, 2], datetime_metrics=["month"]
        ).get_comparison(dialect)
    # Check metric and threshold are the correct way around
    with pytest.raises(TypeError):
        ctl.DateComparison(
            "date", datetime_thresholds=["month"], datetime_metrics=[1]
        ).get_comparison(dialect)
    # Invalid metric
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1], datetime_metrics=["dy"]
        ).get_comparison(dialect)
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[], datetime_metrics=["day"]
        ).get_comparison(dialect)
    # Metric len == 0
    with pytest.raises(ValueError):
        ctl.DateComparison(
            "date", datetime_thresholds=[1], datetime_metrics=[]
        ).get_comparison(dialect)


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
    linker_output = linker.predict().as_pandas_dataframe()

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
    linker_output = linker.predict().as_pandas_dataframe()

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
    linker_output = linker.predict().as_pandas_dataframe()

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
def test_email_comparison_levels(dialect, test_helpers, test_gamma_assert):
    helper = test_helpers[dialect]
    col_name = "email"

    df = pd.DataFrame(
        [
            {"unique_id": 1, "email": "chris@mail.com"},
            {"unique_id": 2, "email": "chris@mail.com"},
            {"unique_id": 3, "email": "chris@othermail.com"},
            {"unique_id": 4, "email": "chrisa@gmail.com"},
            {"unique_id": 5, "email": "chrisa@mali.com"},
            {"unique_id": 6, "email": "chrisa@mailtwo.com"},
            {"unique_id": 7, "email": "chrisat@verydifferentmail.com"},
            {"unique_id": 8, "email": "hcirs@verydifferentmail.com"},
            {"unique_id": 9, "email": "christopher@verydifferentmail.com"},
            {"unique_id": 10, "email": "notchrisarall@mail.com"},
            {"unique_id": 11, "email": "someoneelse@domain.com"},
            {"unique_id": 12, "email": "chrismail.com"},
        ]
    )

    # Generate our various settings objs
    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            ctl.EmailComparison(
                col_name=col_name,
                invalid_emails_as_null=True,
                fuzzy_thresholds=[2],
                fuzzy_metric="damerau_levenshtein",
                include_domain_match_level=True,
            )
        ],
    }

    df = helper.convert_frame(df)
    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker_output = linker.predict().as_pandas_dataframe()

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_level: tuple of ID pairs}
    # size_gamma_lookup = {
    #     9: [(1, 2)],  # Exact match
    #     8: [(1, 3), (2, 3)],  # Exact match on username, different domain
    #     7: [(1, 4), (2, 4)],  # Fuzzy match- full email (lev)
    #     6: [(1, 5), (2, 5)],  # Fuzzy match- full email (dmlev)
    #     5: [(1, 6), (2, 6)],  # Fuzzy match- full email (jw)
    #     4: [(1, 7), (2, 7)],  # Fuzzy match- username only (lev)
    #     3: [(1, 8), (2, 8)],  # Fuzzy match- username only (dmlev)
    #     2: [(1, 9), (2, 9)],  # Fuzzy match- username only (jw)
    #     1: [(1, 10), (2, 10)],  # Domain-only match#
    #     0: [(1, 11), (2, 11)],  # Everything else
    #     -1: [(1, 12)],  # Null level- invalid email
    # }
    size_gamma_lookup = {
        5: [(1, 2)],  # Exact match
        4: [(1, 3), (2, 3)],  # Exact match on username, different domain
        3: [(1, 5), (2, 5), (1, 4), (2, 4)],  # Fuzzy match- full email (dmlev)
        2: [
            (1, 8),
            (2, 8),
            (1, 7),
            (2, 7),
            (1, 6),
            (2, 6),
        ],  # Fuzzy match- username only (dmlev)
        1: [(1, 10), (2, 10)],  # Domain-only match#
        0: [(1, 11), (2, 11), (1, 9), (2, 9)],  # Everything else
        -1: [(1, 12)],  # Null level- invalid email
    }

    test_gamma_assert(linker_output, size_gamma_lookup, col_name)

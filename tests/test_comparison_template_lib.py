import splink.comparison_library as cl
from tests.decorator import mark_with_dialects_excluding
from tests.literal_utils import run_comparison_vector_value_tests


@mark_with_dialects_excluding("postgres", "sqlite")
def test_email_comparison(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.EmailComparison("email"),
            "inputs": [
                {
                    "email_l": "john@smith.com",
                    "email_r": "john@smith.com",
                    "expected_value": 4,
                    "expected_label": "Exact match on email",
                },
                {
                    "email_l": "rebecca@company.com",
                    "email_r": "rebecca@smith.com",
                    "expected_value": 3,
                    "expected_label": "Exact match on username",
                },
                {
                    "email_l": "rebecca@company.com",
                    "email_r": "rebbecca@company.com",
                    "expected_value": 2,
                    "expected_label": "Jaro-Winkler distance of email >= 0.88",
                },
                {
                    "email_l": "rebecca@company.com",
                    "email_r": "rebbecca@xyz.com",
                    "expected_value": 1,
                    "expected_label": "Jaro-Winkler >0.88 on username",
                },
                {
                    "email_l": "john@smith.com",
                    "email_r": "rebbecca@xyz.com",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding("sqlite")
def test_date_of_birth_comparison_levels(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.DateOfBirthComparison(
                "date_of_birth",
                input_is_string=True,
                invalid_dates_as_null=True,
            ),
            "inputs": [
                {
                    "date_of_birth_l": "1990-05-20",
                    "date_of_birth_r": "1990-05-20",
                    "expected_value": 5,
                    "expected_label": "Exact match on date of birth",
                },
                {
                    "date_of_birth_l": "1990-05-01",
                    "date_of_birth_r": "1990-05-11",
                    "expected_value": 4,
                    "expected_label": "DamerauLevenshtein distance <= 1",
                },
                {
                    "date_of_birth_l": "1990-05-20",
                    "date_of_birth_r": "1990-06-19",
                    "expected_value": 3,
                    "expected_label": "Abs date difference <= 1 month",
                },
                {
                    "date_of_birth_l": "1990-05-20",
                    "date_of_birth_r": "1991-04-21",
                    "expected_value": 2,
                    "expected_label": "Abs date difference <= 1 year",
                },
                {
                    "date_of_birth_l": "1990-05-20",
                    "date_of_birth_r": "1999-02-20",
                    "expected_value": 1,
                    "expected_label": "Abs date difference <= 10 year",
                },
                {
                    "date_of_birth_l": "1990-05-20",
                    "date_of_birth_r": "2010-01-17",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_postcode_comparison(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.PostcodeComparison("postcode"),
            "inputs": [
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 1AA",
                    "expected_value": 4,
                    "expected_label": "Exact match on full postcode",
                },
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 1AB",
                    "expected_value": 3,
                    "expected_label": "Exact match on sector",
                },
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW1A 2AA",
                    "expected_value": 2,
                    "expected_label": "Exact match on district",
                },
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "SW2A 1AA",
                    "expected_value": 1,
                    "expected_label": "Exact match on area",
                },
                {
                    "postcode_l": "SW1A 1AA",
                    "postcode_r": "NW1A 1AA",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)

    # Additional test case for PostcodeComparison with lat/long...


@mark_with_dialects_excluding("postgres", "sqlite")
def test_name_comparison(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.NameComparison("name"),
            "inputs": [
                {
                    "name_l": "John",
                    "name_r": "John",
                    "expected_value": 4,
                    "expected_label": "Exact match on name",
                },
                {
                    "name_l": "Stephen",
                    "name_r": "Stephan",
                    "expected_value": 3,
                    "expected_label": "Jaro-Winkler distance of name >= 0.92",
                },
                {
                    "name_l": "Stephen",
                    "name_r": "Steven",
                    "expected_value": 2,
                    "expected_label": "Jaro-Winkler distance of name >= 0.88",
                },
                {
                    "name_l": "Stephen",
                    "name_r": "Steve",
                    "expected_value": 1,
                    "expected_label": "Jaro-Winkler distance of name >= 0.7",
                },
                {
                    "name_l": "Alice",
                    "name_r": "Bob",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding("postgres", "sqlite")
def test_forename_surname_comparison(dialect, test_helpers):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "comparison": cl.ForenameSurnameComparison("forename", "surname"),
            "inputs": [
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected_value": 6,
                    "expected_label": "(Exact match on forename) AND (Exact match on surname)",  # noqa: E501
                },
                {
                    "forename_l": "James",
                    "forename_r": "Smith",
                    "surname_l": "Smith",
                    "surname_r": "James",
                    "expected_value": 5,
                    "expected_label": "Match on reversed cols: forename and surname (both directions)",  # noqa: E501
                },
                {
                    "forename_l": "Stephen",
                    "forename_r": "Stephan",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected_value": 4,
                    "expected_label": "(Jaro-Winkler distance of forename >= 0.92) AND (Jaro-Winkler distance of surname >= 0.92)",  # noqa: E501
                },
                {
                    "forename_l": "Stephen",
                    "forename_r": "Steven",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected_value": 3,
                    "expected_label": "(Jaro-Winkler distance of forename >= 0.88) AND (Jaro-Winkler distance of surname >= 0.88)",  # noqa: E501
                },
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Doe",
                    "surname_r": "Smith",
                    "expected_value": 1,
                    "expected_label": "Exact match on forename",
                },
                {
                    "forename_l": "Alice",
                    "forename_r": "Bob",
                    "surname_l": "Jones",
                    "surname_r": "Smith",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)

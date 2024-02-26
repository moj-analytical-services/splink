import logging
import re

import pandas as pd
import pytest

from splink.blocking_rule_library import block_on
from splink.comparison import Comparison
from splink.comparison_library import LevenshteinAtThresholds
from splink.database_api import DuckDBAPI
from splink.exceptions import ErrorLogger
from splink.linker import Linker
from splink.settings_validation.log_invalid_columns import (
    InvalidColumnSuffixesLogGenerator,
    InvalidTableNamesLogGenerator,
    MissingColumnsLogGenerator,
    check_comparison_for_missing_or_invalid_sql_strings,
    check_for_missing_or_invalid_columns_in_sql_strings,
    check_for_missing_settings_column,
    validate_table_names,
)
from splink.settings_validation.valid_types import (
    log_comparison_errors,
    validate_comparison_levels,
)

from .basic_settings import get_settings_dict

pytestmark = pytest.mark.skip("Until we sort out new style of settings validation")

DF = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
VALID_INPUT_COLUMNS = DF.columns

# TEST PARAMETERS
# Simple Column checks -> evaluate whether a series of columns are missing
test_settings_id_name = "test_id"
missing_settings_column_test_cases = [
    ("unique_id", None),
    ("cluster", None),
    ("", {""}),
    ("invalid column name", {"invalid column name"}),
    ("invalid_column_name", {"invalid_column_name"}),
    (
        ["cluster", "invalid column name", "also_invalid", "cluster"],
        {"also_invalid", "invalid column name"},
    ),
    (["first_name", "full_name", "full_name"], {"full_name"}),
    # Additional test cases
    (
        ["nonexistent_col1", "nonexistent_col2"],
        {"nonexistent_col1", "nonexistent_col2"},
    ),
    ("nonexistent_col", {"nonexistent_col"}),
    (["unique_id", "nonexistent_col"], {"nonexistent_col"}),
    ([""], {""}),  # Edge case: list containing an empty string
    ([], None),  # Edge case: empty list
]

# Blocking rules checks -> Evaluate whether any columns in a blocking rule break our
# validation rules.
blocking_rule_test_cases = {
    "l.surname = r.surname": [],
    "": [],  # handles it gracefully
    "l.first_name = r.first_name and l.dob = r.dob": [],
    "levenshtein(l.email, r.email) <= 2": [],
    "l.invalid_col = r.invalid_col": [MissingColumnsLogGenerator({"invalid_col"})],
    "surhelp = r.ok": [
        MissingColumnsLogGenerator({"surhelp", "ok"}),
        InvalidTableNamesLogGenerator({"surhelp"}),
    ],
    "levenshtein(z.first_name, r.first_name)": [
        InvalidTableNamesLogGenerator({"z.first_name"}),
    ],
    'levenshtein("sur_name", r."sur Name") < 3': [
        MissingColumnsLogGenerator({"sur Name", "sur_name"}),
        InvalidTableNamesLogGenerator({"sur_name"}),
    ],
    "coalesce(l.first_name, NULL) = coalesce(r.first_name, NULL)": [],
    "datediff('day', l.\"dob_test\", r.cluster)": [
        MissingColumnsLogGenerator({"dob_test"}),
    ],
    '"l"."surname" = "r".invalid_name': [MissingColumnsLogGenerator({"invalid_name"})],
    'lower(l."sur_name") = lower(r."surname")': [
        MissingColumnsLogGenerator({"sur_name"})
    ],
    'dmetaphone(c."surname", r."surname")': [
        InvalidTableNamesLogGenerator({"c.surname"})
    ],
    block_on("left", "right")
    .get_blocking_rule("duckdb")
    .blocking_rule_sql: [MissingColumnsLogGenerator({"left", "right"})],
}


# Comparison test cases
email_comparison_to_check = {
    "output_column_name": "email",
    "comparison_levels": [
        {"sql_condition": "email_l IS NULL OR email_r IS NULL"},
        {"sql_condition": "levenshtein(emails_l, test.email_r) < 3"},
        {"sql_condition": "date_diff('day', email_date, email_lz)"},
        {"sql_condition": "ELSE"},
    ],
}
expected_email_comparison_errors = (
    "email",
    {
        "levenshtein(emails_l, test.email_r) < 3": [
            MissingColumnsLogGenerator({"emails"}),
        ],
        "date_diff('day', email_date, email_lz)": [
            MissingColumnsLogGenerator({"email_date", "email_lz"}),
            InvalidColumnSuffixesLogGenerator({"email_date", "email_lz"}),
        ],
    },
)

city_comparison_to_check = {
    "output_column_name": "city",
    "comparison_levels": [
        {"sql_condition": "city_l IS NULL OR city_r IS NULL"},
        {"sql_condition": "not_city_l = not_a_city_r"},
        {"sql_condition": "city_test_l = city_z"},
        # Identical condition - should be ignored by the logger
        {"sql_condition": "city_test_l = city_z"},
        {"sql_condition": "sin(radians(\"city\"['lat']))"},
        {"sql_condition": "ELSE"},
    ],
}
expected_city_comparison_errors = (
    "city",
    {
        "not_city_l = not_a_city_r": [
            MissingColumnsLogGenerator({"not_a_city", "not_city"}),
        ],
        "city_test_l = city_z": [
            MissingColumnsLogGenerator({"city_test", "city_z"}),
            InvalidColumnSuffixesLogGenerator({"city_z"}),
        ],
        "sin(radians(\"city\"['lat']))": [
            InvalidColumnSuffixesLogGenerator({"city"}),
        ],
    },
)


@pytest.mark.parametrize(
    "input_name, expected_output", missing_settings_column_test_cases
)
def test_check_for_missing_settings_column(input_name, expected_output):
    missing_columns = check_for_missing_settings_column(
        settings_id=test_settings_id_name,
        settings_column_to_check=input_name,
        valid_input_dataframe_columns=VALID_INPUT_COLUMNS,
    )
    if expected_output is None:
        assert missing_columns is None
    else:
        assert missing_columns[1] == MissingColumnsLogGenerator(expected_output)


@pytest.mark.parametrize(
    "blocking_rule_sql_string, expected", blocking_rule_test_cases.items()
)
def test_blocking_rule_sql_string_validation(blocking_rule_sql_string, expected):
    result = check_for_missing_or_invalid_columns_in_sql_strings(
        sql_dialect="duckdb",
        sql_strings=[blocking_rule_sql_string],
        valid_input_dataframe_columns=VALID_INPUT_COLUMNS,
        additional_validation_checks=[validate_table_names],
    )

    if expected:
        assert result[blocking_rule_sql_string] == expected
    else:
        assert blocking_rule_sql_string not in result


def test_collective_blocking_rules():
    collective_rules = list(blocking_rule_test_cases.keys())
    expected_output_len = sum(bool(exp) for exp in blocking_rule_test_cases.values())

    result = check_for_missing_or_invalid_columns_in_sql_strings(
        sql_dialect="duckdb",
        sql_strings=collective_rules,
        valid_input_dataframe_columns=VALID_INPUT_COLUMNS,
        additional_validation_checks=[validate_table_names],
    )

    assert len(result) == expected_output_len


def test_identical_blocking_rules_ignored():
    """
    Test to ensure the expected number of errors are raised for collective rules.

    This test checks that the total number of errors identified by the function
    matches the number of expected errors defined in the test cases. It ensures
    that each rule in the collective set raises the expected number of errors.
    """
    test_identical_rules = [
        "datediff('day', l.\"dob_test\", r.cluster)",
        "datediff('day', l.\"dob_test\", r.cluster)",
        "datediff('day', l.\"dob_test\", r.cluster)",
    ]

    result = check_for_missing_or_invalid_columns_in_sql_strings(
        sql_dialect="duckdb",
        sql_strings=test_identical_rules,
        valid_input_dataframe_columns=VALID_INPUT_COLUMNS,
        additional_validation_checks=[validate_table_names],
    )

    # Duplicates should be deleted
    assert len(result) == 1


def test_check_for_missing_or_invalid_columns_in_sql_strings():
    invalid_comparisons_identified = (
        check_comparison_for_missing_or_invalid_sql_strings(
            sql_dialect="duckdb",
            comparisons_to_check=[
                Comparison(**email_comparison_to_check, sqlglot_dialect_name="duckdb"),
                Comparison(**city_comparison_to_check, sqlglot_dialect_name="duckdb"),
                LevenshteinAtThresholds("first_name").get_comparison("duckdb"),
            ],
            valid_input_dataframe_columns=VALID_INPUT_COLUMNS,
        )
    )

    expected_outputs = (
        expected_email_comparison_errors,
        expected_city_comparison_errors,
    )

    for check, expected in zip(invalid_comparisons_identified, expected_outputs):
        assert check == expected, f"Failed comparison check: {check}"


# Integration test to assess if the logs are working as expected
def test_settings_validation_logs(caplog):
    settings = get_settings_dict()
    # Inject some basic errors
    settings["unique_id_column_name"] = "abcde"
    settings["additional_columns_to_retain"] = ["abcde"]
    settings["blocking_rules_to_generate_predictions"] = ["l.abcde = z.abcde"]
    settings["comparisons"][3] = LevenshteinAtThresholds("abcde")

    # Execute the DuckDBLinker to generate logs
    with caplog.at_level(logging.WARNING):
        db_api = DuckDBAPI()

        Linker(DF, settings, validate_settings=True, database_api=db_api)

        # Define expected log segments
        expected_log_segments = [
            (
                "Setting: `unique_id_column_name`",
                "Missing column(s) from input dataframe(s): `abcde`",
            ),
            (
                "Setting: `additional_columns_to_retain`",
                "Missing column(s) from input dataframe(s): `abcde`",
            ),
            (
                "Invalid Columns(s) in Blocking Rule(s)",
                "Missing column(s) from input dataframe(s): `abcde`",
            ),
            (
                "Invalid Columns(s) in Blocking Rule(s)",
                "Invalid table names provided (only `l.` and `r.` are valid): `z.abcde`",  # noqa: E501
            ),
            (
                "Invalid Columns(s) in Comparison(s)",
                "Missing column(s) from input dataframe(s): `abcde`",
            ),
            # Add more log segments if needed
        ]

        # Check each expected log segment in the captured logs
        for header, error in expected_log_segments:
            assert header in caplog.text and error in caplog.text


def test_comparison_validation():
    import splink.comparison_level_library as cll
    import splink.comparison_library as cl
    from splink.exceptions import InvalidDialect

    # Check blank settings aren't flagged
    # Trimmed settings (settings w/ only the link type, for example)
    # are tested elsewhere.
    db_api = DuckDBAPI()

    Linker(
        pd.DataFrame({"a": [1, 2, 3]}),
        {"link_type": "dedupe_only"},
        database_api=db_api,
    )

    settings = get_settings_dict()

    # Contents aren't tested as of yet
    email_no_comp_level = {"comparison_lvls": []}
    # cll instead of cl
    email_cc = cll.ExactMatchLevel("email").get_comparison_level("duckdb")
    settings["comparisons"][3] = email_cc
    # random str
    settings["comparisons"][4] = "help"
    # missing key dict key and replaced w/ `comparison_lvls`
    settings["comparisons"].append(email_no_comp_level)
    # a comparison containing another comparison
    settings["comparisons"].append(
        {
            "comparison_levels": [
                cll.NullLevel("test"),
                # Invalid Spark cll
                cl.ExactMatch("test"),
                cll.ElseLevel(),
            ]
        }
    )

    # a comparison containing another comparison
    settings["comparisons"].append(
        {
            "comparison_levels": [
                cll.NullLevel("test"),
                # Invalid Spark cll
                cl.ExactMatch("test"),
                cll.ElseLevel(),
            ]
        }
    )

    log_comparison_errors(None, "duckdb")  # confirm it works with None as an input...

    # Init the error logger. This is normally handled in
    # `log_comparison_errors`, but here we want to capture the
    # errors instead of logging.
    error_logger = ErrorLogger()
    error_logger = validate_comparison_levels(
        error_logger, settings["comparisons"], "duckdb"
    )

    # Check our errors are raised
    errors = error_logger.raw_errors
    # -3 as we have three valid comparisons
    assert len(error_logger.raw_errors) == len(settings["comparisons"]) - 3

    # These errors are raised in the order they are defined in the settings
    expected_errors = (
        (TypeError, "is a comparison level"),
        (TypeError, "is of an invalid data type."),
        (SyntaxError, "missing the required `comparison_levels`"),
        (InvalidDialect, "within its comparison levels - spark."),
        (InvalidDialect, re.compile(r".*(presto.*spark|spark.*presto).*")),
        (TypeError, "contains the following invalid levels"),
    )

    for n, (e, txt) in enumerate(expected_errors):
        if isinstance(txt, re.Pattern):
            # If txt is a compiled regular expression, use re.search
            with pytest.raises(e) as exc_info:
                raise errors[n]
            assert txt.search(str(exc_info.value)), f"Regex did not match for error {n}"
        else:
            with pytest.raises(e, match=txt):
                raise errors[n]

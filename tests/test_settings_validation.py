import logging
from copy import deepcopy

import pandas as pd
import pytest

from splink.convert_v2_to_v3 import convert_settings_from_v2_to_v3
from splink.duckdb.linker import DuckDBLinker
from splink.exceptions import ErrorLogger
from splink.settings_validation.column_lookups import (
    InvalidCols,
    InvalidColumnsLogger,
)
from splink.settings_validation.valid_types import (
    log_comparison_errors,
    validate_comparison_levels,
)

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


def alter_settings(linker, new_settings):
    linker = deepcopy(linker)
    linker.load_settings(new_settings)
    return InvalidColumnsLogger(linker)


def verify_single_setting_validation(
    invalid_cols_tuple,
    settings_reference,
    invalid_type,
    invalid_columns,
):
    """A quick checker for any settings objects not
    containing extensive SQL statements that require
    validation.

    Args:
        invalid_cols_tuple (tuple): The output from
            `InvalidColumnsLogger(linker).validate_{}`.
        settings_reference (str): The reference assigned
            to that given validation check. See the `InvalidColumnsLogger`
            class for more.
        invalid_type (str): One of: `invalid_cols`, `invalid_table_pref`
            or `invalid_col_suffix`.
        invalid_columns (list): A list of the expected invalid columns
    """
    assert invalid_cols_tuple[0] == settings_reference
    inv_cols = invalid_cols_tuple[1]
    i_type, cols = inv_cols.invalid_type, inv_cols.invalid_columns
    assert i_type == invalid_type
    assert len(invalid_columns) == len(cols)
    assert all(item in invalid_columns for item in cols)


def verify_complicated_settings_objects(invalid, expected):
    """This is used for validating BRs and CLs. As they take advantage
    of sets to remove duplicates, `expected_cols` the order is not consistent
    and cannot be determined ahead of time.

    Args:
        invalid_cols_tuple (tuple): The output from
            `InvalidColumnsLogger(linker).validate_{}`.
        settings_reference (str): The reference assigned
            to that given validation check. See the `InvalidColumnsLogger`
            class for more.
        invalid_type (str): One of: `invalid_cols`, `invalid_table_pref`
            or `invalid_col_suffix`.
        invalid_columns (list): A list of the expected invalid columns
    """
    for brs, out in zip(invalid.items(), expected.items()):
        # dictionary key, this should be a br
        assert brs[0] == out[0]

        # This grabs all of our `InvalidCols` classes
        for br_invalid, exp_invalid in zip(brs[1], out[1]):
            assert br_invalid.invalid_type == exp_invalid.invalid_type
            assert (
                br_invalid.invalid_columns.sort() == exp_invalid.invalid_columns.sort()
            )


@mark_with_dialects_excluding("sqlite", "postgres")
def test_invalid_cols_detected(test_helpers, dialect):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Create random settings to check outputs with
    settings = get_settings_dict()
    linker = Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    settings_logger = InvalidColumnsLogger(linker)

    # Check that `invalid_cols_detected` is triggered correctly
    assert settings_logger.invalid_cols_detected is False
    settings_logger.valid_uid = ()
    assert settings_logger.invalid_cols_detected is False
    settings_logger.valid_uid = ["testing"]
    assert settings_logger.invalid_cols_detected is True


@mark_with_dialects_excluding("sqlite", "postgres", "spark")
def test_unique_id_settings_validation(test_helpers, dialect, caplog):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Create random settings to check outputs with
    settings = get_settings_dict()
    linker = Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    settings_logger = InvalidColumnsLogger(linker)

    #############
    # UNIQUE ID #
    #############
    # Nothing supplied, we expect a value of None to be returned
    assert settings_logger.validate_uid is None

    # Valid column
    settings["unique_id_column_name"] = "unique_id"
    uid_check = alter_settings(linker, settings).validate_uid
    assert uid_check is None

    # Add additional faulty columns to retain
    invalid_id = "invalid column name"
    settings["unique_id_column_name"] = invalid_id
    uid_check = alter_settings(linker, settings)
    verify_single_setting_validation(
        uid_check.validate_uid,
        "unique_id_column_name",
        "invalid_cols",
        [invalid_id],
    )

    # Check logger formatting is approximately what we expect...
    with caplog.at_level(logging.WARNING):
        uid_check.construct_output_logs()
        str_header = (
            "Setting: `unique_id_column_name`\n"
            "======================================\n"
        )
        str_error = (
            "       - Missing column(s) from input dataframe(s): " f"`{invalid_id}`\n"
        )
        for string in [str_header, str_error]:
            assert string in caplog.text


@mark_with_dialects_excluding("sqlite", "postgres", "spark")
def test_retained_cols_settings_validation(test_helpers, dialect, caplog):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Create random settings to check outputs with
    settings = get_settings_dict()
    linker = Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    settings_logger = InvalidColumnsLogger(linker)

    ##################
    # COLS TO RETAIN #
    ##################
    # Only "cluster" supplied. As such, we expect a value of None to be returned
    settings_logger = InvalidColumnsLogger(linker)
    assert settings_logger.validate_cols_to_retain is None

    # Add additional faulty columns to retain
    exp_cols_to_retain = ["also_invalid", "invalid column name"]
    settings["additional_columns_to_retain"] = [
        "cluster",
        "invalid column name",
        "also_invalid",
        "cluster",  # duplicate - check it is ignored
    ]
    c_to_retain = alter_settings(linker, settings)
    verify_single_setting_validation(
        c_to_retain.validate_cols_to_retain,
        "additional_columns_to_retain",
        "invalid_cols",
        exp_cols_to_retain,
    )
    # Check logger formatting is approximately what we expect...
    with caplog.at_level(logging.WARNING):
        c_to_retain.construct_output_logs()
        str_header = (
            "Setting: `additional_columns_to_retain`\n"
            "======================================\n"
        )
        # column names are stored in a hashset, so we can't check for exact equality
        # including the column names
        str_error = "       - Missing column(s) from input dataframe(s): `"
        for string in [str_header, str_error]:
            assert string in caplog.text


@mark_with_dialects_excluding("sqlite", "postgres", "spark")
def test_blocking_rule_settings_validation(test_helpers, dialect, caplog):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Create random settings to check outputs with
    settings = get_settings_dict()
    linker = Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    settings_logger = InvalidColumnsLogger(linker)

    ##################
    # BLOCKING RULES #
    ##################
    # perfectly valid rules
    assert len(settings_logger.validate_blocking_rules) == 0

    initial_blocking_rules = [
        "l.surname = r.surname",
        "l.invalid_col = r.invalid_col",
        "surhelp = r.ok",
    ]
    settings["blocking_rules_to_generate_predictions"] = initial_blocking_rules
    invalid_brs = alter_settings(linker, settings).validate_blocking_rules

    invalid_col_br = [
        InvalidCols(invalid_type="invalid_cols", invalid_columns=["invalid_col"]),
    ]
    surhelp_br = [
        InvalidCols(invalid_type="invalid_cols", invalid_columns=["surhelp", "ok"]),
        InvalidCols(invalid_type="invalid_table_pref", invalid_columns=["surhelp"]),
    ]

    invalid_rules = (invalid_col_br, surhelp_br)
    # pop the first br from initial_blocking_rules as it is valid
    expected_out = {
        br: inv_cols for br, inv_cols in zip(initial_blocking_rules[1:], invalid_rules)
    }
    # Check both dictionaries are identical
    verify_complicated_settings_objects(invalid_brs, expected_out)

    # DuckDB quote syntax added in
    blocking_rules_to_check = [
        'levenshtein("sur_name", r."sur Name") < 3',
        "coalesce(l.first_name, NULL) = coalesce(first_name, NULL)",
        "datediff('day', l.\"dob_test\", r.cluster)",
        # Identical rule - should be ignored
        "datediff('day', l.\"dob_test\", r.cluster)",
    ]
    settings["blocking_rules_to_generate_predictions"] = blocking_rules_to_check
    invalid_brs = alter_settings(linker, settings)

    lev_br = [
        InvalidCols(
            invalid_type="invalid_cols", invalid_columns=["sur_name", "sur Name"]
        ),
        InvalidCols(invalid_type="invalid_table_pref", invalid_columns=["sur_name"]),
    ]
    coal_br = InvalidCols(
        invalid_type="invalid_table_pref", invalid_columns=["first_name"]
    )
    datediff_br = InvalidCols(invalid_type="invalid_cols", invalid_columns=["dob_test"])
    invalid_rules = (lev_br, [coal_br], [datediff_br])
    expected_out = {
        br: inv_cols for br, inv_cols in zip(blocking_rules_to_check, invalid_rules)
    }
    # Check both dictionaries are identical
    verify_complicated_settings_objects(
        invalid_brs.validate_blocking_rules, expected_out
    )

    # Check logger formatting is approximately what we expect...
    with caplog.at_level(logging.WARNING):
        invalid_brs.construct_output_logs()

        str_header = (
            "Invalid Columns(s) in Blocking Rule(s)\n"
            "======================================\n"
        )
        # column names are stored in a hashset, so we can't check for exact equality
        # where we have multiple column names
        missing_cols = "       - Missing column(s) from input dataframe(s): `dob_test`"
        invalid_prefix = (
            "       - Invalid table prefixes (only `l.` and `r.` are valid): "
            "`first_name`"
        )
        for string in [str_header, missing_cols, invalid_prefix]:
            # for string in [str_header, missing_cols]:
            assert string in caplog.text


@mark_with_dialects_excluding("sqlite", "postgres", "spark")
def test_comparison_settings_validation(test_helpers, dialect, caplog):
    helper = test_helpers[dialect]
    Linker = helper.Linker
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Create random settings to check outputs with
    settings = get_settings_dict()
    linker = Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )
    settings_logger = InvalidColumnsLogger(linker)

    #####################
    # COMPARISON LEVELS #
    #####################
    # perfectly valid comparison levels from our test settings
    assert len(settings_logger.validate_comparison_levels) == 0

    # Create some wonky comparison levels
    email_cc = {
        "output_column_name": "email",
        "comparison_levels": [
            {
                "sql_condition": "email_l IS NULL OR email_r IS NULL",
                "label_for_charts": "Comparison includes null",
                "is_null_level": True,
            },
            {
                "sql_condition": "levenshtein(emails_l, test.email_r) < 3",
                "label_for_charts": "Levenshtein < 3",
            },
            {
                "sql_condition": "date_diff('day', email_date_l, email_r)",
                "label_for_charts": "Random date diff",
            },
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
            },
        ],
    }

    city_cc = {
        "output_column_name": "city",
        "comparison_levels": [
            {
                "sql_condition": "city_l IS NULL OR city_r IS NULL",
                "label_for_charts": "Comparison includes null",
                "is_null_level": True,
            },
            {
                "sql_condition": "not_city_l = not_a_city_r",
                "label_for_charts": "Invalid exact SQL",
            },
            {
                "sql_condition": "city_test_l = city_r",
                "label_for_charts": "Exact match",
            },
            # Identical condition - should be ignored by the logger
            {
                "sql_condition": "city_test_l = city_r",
                "label_for_charts": "Exact match",
            },
            {
                # missing _l suffix
                "sql_condition": "sin(radians(\"city\"['lat']))",
                "label_for_charts": "Exact match",
            },
            {
                "sql_condition": "ELSE",
                "label_for_charts": "All other comparisons",
            },
        ],
    }

    settings["comparisons"][3:] = [email_cc, city_cc]
    invalid_cls = alter_settings(linker, settings)

    expected_email_invalid_output = {
        "levenshtein(emails_l, test.email_r) < 3": [
            InvalidCols(invalid_type="invalid_cols", invalid_columns=["emails"])
        ],
        "date_diff('day', email_date_l, email_r)": [
            InvalidCols(invalid_type="invalid_cols", invalid_columns=["email_date"])
        ],
    }

    expected_city_invalid_output = {
        "not_city_l = not_a_city_r": [
            InvalidCols(
                invalid_type="invalid_cols", invalid_columns=["not_a_city", "not_city"]
            )
        ],
        "city_test_l = city_r": [
            InvalidCols(invalid_type="invalid_cols", invalid_columns=["city_test"])
        ],
        "sin(radians(\"city\"['lat']))": [
            InvalidCols(invalid_type="invalid_col_suffix", invalid_columns=["city"])
        ],
    }

    # invalid_cls indexing is:
    # [0] - output column name
    # [1] - list of SQL statements and invalid column tuples
    # Dicts can't be used as output column name is not a required variable
    verify_complicated_settings_objects(
        invalid_cls.validate_comparison_levels[0][1], expected_email_invalid_output
    )
    verify_complicated_settings_objects(
        invalid_cls.validate_comparison_levels[1][1], expected_city_invalid_output
    )
    # Check logger formatting is approximately what we expect...
    with caplog.at_level(logging.WARNING):
        invalid_cls.construct_output_logs()

        str_header = (
            "Invalid Columns(s) in Comparison(s)\n"
            "======================================\n"
        )
        missing_cols = (
            "\n       - Missing column(s) from input dataframe(s): " "`city_test`\n"
        )
        invalid_prefix = (
            "\n       - Invalid table suffixes (only `_l` and `_r` are valid): "
            "`city`\n"
        )
        for string in [str_header, missing_cols, invalid_prefix]:
            assert string in caplog.text


def test_settings_validation_on_2_to_3_converter():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # Trial with settings converter
    sql_custom_name = """
    case
        when (col_1 is null or col_1_r is null) and
                (surname_l is null or surname_r is null)  then -1
        when forename_l = forename_r and surname_l = surname_r then 3
        when forename_l = forename_r then 2
        when surname_l = surname_r then 1
        else 0
    end
    """
    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [
            {
                "custom_name": "name_inversion_forname",
                "case_expression": sql_custom_name,
                "custom_columns_used": ["forename", "surname"],
                "num_levels": 4,
            },
        ],
        "blocking_rules": ["l.first_name = r.first_name"],
    }

    converted = convert_settings_from_v2_to_v3(settings)
    linker = DuckDBLinker(
        df,
        converted,
    )
    i_logger = InvalidColumnsLogger(linker)
    # Longer form output without an output column name
    assert i_logger.validate_comparison_levels == [
        (
            None,
            {
                (
                    "(col_1 IS NULL OR col_1_r IS NULL)"
                    " AND "
                    "(surname_l IS NULL OR surname_r IS NULL)"
                ): [
                    InvalidCols(invalid_type="invalid_cols", invalid_columns=["col_1"]),
                    InvalidCols(
                        invalid_type="invalid_col_suffix", invalid_columns=["col_1"]
                    ),
                ],
                "forename_l = forename_r AND surname_l = surname_r": [
                    InvalidCols(
                        invalid_type="invalid_cols", invalid_columns=["forename"]
                    )
                ],
                "forename_l = forename_r": [
                    InvalidCols(
                        invalid_type="invalid_cols", invalid_columns=["forename"]
                    )
                ],
            },
        )
    ]


def test_validate_sql_dialect():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {"link_type": "link_and_dedupe", "sql_dialect": "spark"}

    with pytest.raises(Exception) as excinfo:
        DuckDBLinker(
            df,
            settings,
        )
    assert str(excinfo.value) == (
        "Incompatible SQL dialect! `settings` dictionary uses dialect "
        "spark, but expecting 'duckdb' for Linker of type `DuckDBLinker`"
    )


def test_comparison_validation():
    import splink.athena.comparison_level_library as ath_cll
    import splink.duckdb.comparison_level_library as cll
    import splink.spark.comparison_level_library as sp_cll
    from splink.exceptions import InvalidDialect
    from splink.spark.comparison_library import exact_match

    # Check blank settings aren't flagged
    # Trimmed settings (settings w/ only the link type, for example)
    # are tested elsewhere.
    DuckDBLinker(
        pd.DataFrame({"a": [1, 2, 3]}),
    )

    settings = get_settings_dict()

    # Contents aren't tested as of yet
    email_no_comp_level = {
        "comparison_lvls": [],
    }

    # cll instead of cl
    email_cc = cll.exact_match_level("email")
    settings["comparisons"][3] = email_cc
    # random str
    settings["comparisons"][4] = "help"
    # missing key dict key and replaced w/ `comparison_lvls`
    settings["comparisons"].append(email_no_comp_level)
    # Check invalid import is detected
    settings["comparisons"].append(exact_match("test"))
    # mismashed comparison
    settings["comparisons"].append(
        {
            "comparison_levels": [
                sp_cll.null_level("test"),
                # Invalid Spark cll
                ath_cll.exact_match_level("test"),
                cll.else_level(),
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
    assert len(error_logger.raw_errors) == len(settings["comparisons"]) - 3

    # Our expected error types and part of the corresponding error text
    expected_errors = (
        (TypeError, "is a comparison level"),
        (TypeError, "is of an invalid data type."),
        (SyntaxError, "missing the required `comparison_levels`"),
        (InvalidDialect, "within its comparison levels - spark."),
        (InvalidDialect, "within its comparison levels - presto, spark."),
    )

    for n, (e, txt) in enumerate(expected_errors):
        with pytest.raises(e, match=txt):
            raise errors[n]

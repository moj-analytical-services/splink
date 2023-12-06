from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, List

import sqlglot

from ..comparison import Comparison
from ..parse_sql import parse_columns_in_sql
from .settings_column_cleaner import (
    SettingsColumnCleaner,
    clean_and_find_columns_not_in_input_dfs,
    find_columns_not_in_input_dfs,
)
from .settings_validation_log_strings import (
    InvalidColumnSuffixesLogGenerator,
    InvalidTableNamesLogGenerator,
    MissingColumnsLogGenerator,
    construct_missing_column_in_blocking_rule_log,
    construct_missing_column_in_comparison_level_log,
    construct_missing_settings_column_log,
)

if TYPE_CHECKING:
    from .settings_validation_log_strings import InvalidColumnsLogGenerator


logger = logging.getLogger(__name__)


def validate_table_names(
    columns_to_check: list[sqlglot.expressions],
) -> InvalidColumnsLogGenerator:
    """Validate a series of table names assigned to columns extracted from
    SQL statements. We expect all columns to be assigned either a `l` or
    `r` prefix.
    """
    # list of valid columns
    invalid_columns = [c.sql() for c in columns_to_check if c.table not in ["l", "r"]]
    return InvalidTableNamesLogGenerator(set(invalid_columns))


def validate_column_suffixes(
    columns_to_check: list[sqlglot.expressions],
) -> InvalidColumnsLogGenerator:
    """Validate a series of column suffixes. We expect columns to be suffixed
    with either `_l` or `_r`. Where this is missing, flag it as an error.
    """
    # list of valid columns
    invalid_columns = [
        c.sql() for c in columns_to_check if not c.sql().endswith(("_l", "_r"))
    ]
    return InvalidColumnSuffixesLogGenerator(set(invalid_columns))


def check_for_missing_settings_column(
    settings_id: str,
    settings_column_to_check: set,
    valid_input_dataframe_columns: list,
):
    """Validate simple settings columns with strings as input.
    i.e. Anything that doesn't require SQL to be parsed.
    """
    missing_columns = find_columns_not_in_input_dfs(
        valid_input_dataframe_columns, columns_to_check=settings_column_to_check
    )

    if missing_columns:
        # If the column is missing, return an InvalidColumnsTracker
        return settings_id, MissingColumnsLogGenerator(missing_columns)


def check_for_missing_or_invalid_columns_in_sql_strings(
    sql_dialect: str,
    sql_strings: list[str],
    valid_input_dataframe_columns: list[str],
    additional_validation_checks: List[Callable] = [],
) -> list[MissingColumnsLogGenerator]:
    """Evaluate whether the column(s) supplied in a series of SQL strings
    exist in our raw data.

    This is used to assess whether a blocking rule or comparison level
    contains columns that exist in the underlying dataset(s) supplied.

    Args:
        sql_dialect (str): The SQL dialect in use.
        sql_strings (list[str]): A list of valid SQL strings.
        valid_input_dataframe_columns (list[str]): The list of columns
            identified in your input dataframe(s).
        additional_validation_checks (List[Callable]): Functions used to
            check the parsed SQL string. These can currently include
            `validate_table_names`, and `validate_column_suffixes`.
    """

    validation_dict = {}
    for sql_string in sql_strings:
        # `parse_columns_in_sql` also checks if our sql string is parseable
        identified_columns_in_sql = parse_columns_in_sql(
            sql_string, sql_dialect=sql_dialect
        )
        if not identified_columns_in_sql:
            continue

        # Chech whether our list of identified columns have any invalid features.
        # These can be:
        # - A column that does not exist in the input dataframe(s)
        # - A column with an invalid prefix
        # - A column with an invalid suffix
        invalid_column_tracker = []

        # Check for missing columns
        missing_columns = clean_and_find_columns_not_in_input_dfs(
            valid_input_dataframe_columns=valid_input_dataframe_columns,
            sqlglot_tree_columns_to_check=identified_columns_in_sql,
            sql_dialect=sql_dialect,
        )
        if missing_columns:
            missing_columns = MissingColumnsLogGenerator(missing_columns)
            invalid_column_tracker.append(missing_columns)

        # Run any additional validations checks.
        # Skipped if no additional checks are requested
        for validation_check_to_run in additional_validation_checks:
            validated_columns = validation_check_to_run(
                columns_to_check=identified_columns_in_sql
            )
            # Check to see if any any invalid or missing columns were found
            # and log them in the tracker
            if validated_columns.invalid_columns:
                invalid_column_tracker.append(validated_columns)

        if (
            invalid_column_tracker
        ):  # only append our tracker if we have identified errors
            validation_dict[sql_string] = invalid_column_tracker

    return validation_dict


def check_comparison_for_missing_or_invalid_sql_strings(
    sql_dialect: str,
    comparisons_to_check: list[Comparison],
    valid_input_dataframe_columns: list[str],
) -> list[MissingColumnsLogGenerator]:
    """Split apart the comparison levels found within a comparison
    and review the SQL contained within.

    If any errors are identified, log them in the invalid_column_tracker.
    """
    invalid_column_tracker = []
    for comparison in comparisons_to_check:
        comp_dict = comparison.as_dict()
        comparison_level_sql_strings = [
            cl["sql_condition"] for cl in comp_dict["comparison_levels"]
        ]

        invalid_comparison_levels = check_for_missing_or_invalid_columns_in_sql_strings(
            sql_dialect=sql_dialect,
            sql_strings=comparison_level_sql_strings,
            valid_input_dataframe_columns=valid_input_dataframe_columns,
            additional_validation_checks=[validate_column_suffixes],
        )
        if invalid_comparison_levels:
            invalid_column_tracker.append(
                (comp_dict.get("output_column_name"), invalid_comparison_levels)
            )

    return invalid_column_tracker


class InvalidColumnsLogger:
    """
    Construct a series of log strings indicating where columns in a settings object
    are missing from the user's input dataframe(s).
    """

    def __init__(self, cleaned_settings: SettingsColumnCleaner):
        self.cleaned_settings_values = cleaned_settings

    def validate_uid(self):
        return check_for_missing_settings_column(
            settings_id="unique_id_column_name",
            settings_column_to_check=self.cleaned_settings_values.uid,
            valid_input_dataframe_columns=self.cleaned_settings_values.input_columns,
        )

    def validate_cols_to_retain(self):
        return check_for_missing_settings_column(
            settings_id="additional_columns_to_retain",
            settings_column_to_check=self.cleaned_settings_values.cols_to_retain,
            valid_input_dataframe_columns=self.cleaned_settings_values.input_columns,
        )

    def validate_blocking_rules(self):
        return check_for_missing_or_invalid_columns_in_sql_strings(
            sql_dialect=self.cleaned_settings_values.sql_dialect,
            sql_strings=self.cleaned_settings_values.blocking_rules,
            valid_input_dataframe_columns=self.cleaned_settings_values.input_columns,
            additional_validation_checks=[validate_table_names],
        )

    def validate_comparison_levels(self):
        return check_comparison_for_missing_or_invalid_sql_strings(
            sql_dialect=self.cleaned_settings_values.sql_dialect,
            comparisons_to_check=self.cleaned_settings_values.comparisons,
            valid_input_dataframe_columns=self.cleaned_settings_values.input_columns,
        )

    def construct_output_logs(self, run_settings_validations=True):
        if not run_settings_validations:
            return

        missing_uid = self.validate_uid()
        missing_cols_to_retain = self.validate_cols_to_retain()
        invalid_blocking_rules = self.validate_blocking_rules()
        invalid_comparison_levels = self.validate_comparison_levels()

        # Generate our log strings
        log_strings = (
            construct_missing_settings_column_log(missing_uid),
            construct_missing_settings_column_log(missing_cols_to_retain),
            construct_missing_column_in_blocking_rule_log(invalid_blocking_rules),
            construct_missing_column_in_comparison_level_log(invalid_comparison_levels),
        )
        # Remove anything that doesn't need validation
        log_strings = [log_string for log_string in log_strings if log_string]

        if log_strings:  # if nothing needs logging, return
            # Initial warning for the logger
            logger.warning(
                "SETTINGS VALIDATION: Errors were identified in your "
                "settings dictionary. \n"
            )

            for log_string in log_strings:
                logger.warning(log_string)

            logger.warning(
                "You may want to verify your settings dictionary has "
                "valid inputs in all fields before continuing."
            )

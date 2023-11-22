from __future__ import annotations

import logging
from copy import deepcopy
from typing import NamedTuple

import sqlglot

from ..parse_sql import parse_columns_in_sql
from .settings_validator import SettingsValidator

logger = logging.getLogger(__name__)


class InvalidColValidator(SettingsValidator):
    """An invalid columns validator. This class aims to identify any
    columns outlined in a user's settings object, that do not exist
    in their underlying dataframe(s).

    This is an extension of the `SettingsValidator` class, which extracts
    key values from a settings dictionary and contains some core cleaning
    functions.

    Args:
        SettingsValidator: The central settings validation class,
            containing key values from a settings dictionary and
            some core cleaning functionality.
    """

    def return_missing_columns(self, cols_to_check: set) -> InvalidCols:
        """Identify missing columns in the input dataframe(s). This function
        does not apply any cleaning to the input column(s).

        Args:
            cols_to_check (set): A list of columns to check for the
                existence of.

        Returns:
            list: Returns a list of all input columns not found within
                your raw input tables.
        """
        # the key to use when producing our warning logs
        invalid_type = "invalid_cols"
        missing_cols = [col for col in cols_to_check if col not in self.input_columns]
        return InvalidCols(invalid_type, missing_cols)

    def clean_and_return_missing_columns(
        self, cols: list[sqlglot.expressions]
    ) -> list[InvalidCols]:
        """Clean a list of sqlglot column names to remove the prefix (l.)
        and suffix (_l) and then return any that are missing from the
        input dataframe(s).

        Args:
            cols (list[sqlglot.expressions]): A list of columns given as
                sqlglot expressions

        Returns:
            list: Returns a list of all input columns not found within
                your raw input tables.
        """
        cleaned_cols = set(self.remove_prefix_and_suffix_from_column(c) for c in cols)
        return self.return_missing_columns(cleaned_cols)

    def validate_table_names(self, cols: list[sqlglot.expressions]) -> InvalidCols:
        """Validate a series of table names assigned to columns extracted from
        SQL statements. Here, we expect all columns to be assigned either a `l` or
        `r` prefix.

        Args:
            cols (list[sqlglot.expressions]): A list of columns given as
                sqlglot expressions

        Returns:
            InvalidCols: An InvalidCols instance with the `invalid_type`
                and a list of invalid columns
        """
        # the key to use when producing our warning logs
        invalid_type = "invalid_table_pref"
        # list of valid columns
        invalid_cols = [c.sql() for c in cols if c.table not in ["l", "r"]]
        return InvalidCols(invalid_type, invalid_cols)

    def validate_column_suffixes(self, cols: list[sqlglot.expressions]) -> InvalidCols:
        """Validate a series of column suffixes. Here, we expect columns to be suffixed
        with either `_l` or `_r`. Where this is missing, flag it as an error.

        Args:
            cols (list[sqlglot.expressions]): A list of columns given as
                sqlglot expressions

        Returns:
            InvalidCols: An InvalidCols instance with the `invalid_type`
                and a list of invalid columns
        """
        # the key to use when producing our warning logs
        invalid_type = "invalid_col_suffix"
        # list of valid columns
        invalid_cols = [c.sql() for c in cols if not c.sql().endswith(("_l", "_r"))]
        return InvalidCols(invalid_type, invalid_cols)

    def validate_columns_in_sql_strings(
        self,
        sql_strings: list[str],
        checks: list,
    ) -> list[InvalidCols]:
        """Evaluate whether the column(s) supplied in a series of SQL strings
        exist in our raw data.

        This is used to assess whether a blocking rule or comparison level
        contains columns that exist in the underlying dataset(s) supplied.

        Args:
            sql_strings (list[str]): A list of valid SQL strings
            checks (list[function]): The functions used to check the parsed
                sql string. These can be: `clean_and_return_missing_columns`,
                `validate_table_names` and `validate_column_suffixes`

        Returns:
            list[InvalidCols]: A list of InvalidCols classes, denoting the
                the `invalid_type` and a list of the invalid columns that were
                identified.
        """

        validation_dict = {}
        for sql_string in sql_strings:
            cols = parse_columns_in_sql(sql_string, sql_dialect=self._sql_dialect)
            if not cols:
                continue  # checks if our sql string is parseable
            # Collect trees for checks and filter only those that
            # contain invalid columns
            invalid_column_trees = [
                # deepcopy to ensure our syntax trees aren't manipulated
                inv_cols
                for inv_cols in (check(deepcopy(cols)) for check in checks)
                if inv_cols.contains_invalid_columns
            ]

            if invalid_column_trees:
                validation_dict[sql_string] = invalid_column_trees

        return validation_dict

    def validate_settings_column(self, settings_id, cols: set):
        """Validate simple settings columns with strings as input.
        i.e. Anything that doesn't require SQL to be parsed.

        Args:
            settings_id (str): The setting ID within your underlying
                `settings_dict`.
            cols (set): All columns found within your input dataframe(s).

        Returns:
            (settings_id, missing_cols): Returns your settings ID and any
                columns identified as missing.
        """
        missing_cols = self.return_missing_columns(cols)
        # The `contains_invalid_columns` check simply tests to see if any values have
        # been flagged. If there are no invalid cols, return None.
        if missing_cols.contains_invalid_columns:
            return settings_id, missing_cols


class InvalidColumnsLogger(InvalidColValidator):
    """Takes the methods created in `InvalidColValidator`
    and assess them to evaluate whether any columns included in the
    user's settings dictionary are invaid.
    """

    def __init__(self, linker):
        self.settings_validator = super().__init__(linker)
        self.input_columns = self._input_columns

        # These are extracted in the inherited
        # SettingsValidator class
        self.valid_uid = self.validate_uid
        self.valid_cols_to_retain = self.validate_cols_to_retain
        self.invalid_brs = self.validate_blocking_rules
        self.invalid_cls = self.validate_comparison_levels

    @property
    def invalid_cols_detected(self):
        # Check if any of our evaluations yield invalid
        # columns.
        return not all(
            (
                not self.valid_uid,
                not self.valid_cols_to_retain,
                not self.invalid_brs,
                not self.invalid_cls,
            )
        )

    @property
    def validate_uid(self):
        return self.validate_settings_column(
            settings_id="unique_id_column_name",
            cols=self.uid,
        )

    @property
    def validate_cols_to_retain(self):
        return self.validate_settings_column(
            settings_id="additional_columns_to_retain",
            cols=self.cols_to_retain,
        )

    @property
    def validate_blocking_rules(self):
        return self.validate_columns_in_sql_strings(
            sql_strings=self.blocking_rules,
            checks=(
                # Check column validity and for invalid table names
                self.clean_and_return_missing_columns,
                self.validate_table_names,
            ),
        )

    @property
    def validate_comparison_levels(self):
        # See docstring for `validate_columns_in_sql_string_dict_comp`.
        invalid_col_tracker = []
        for comparisons in self.comparisons:
            # pull out comparison dict
            comp_dict = comparisons._comparison_dict
            sql_strings = [cl["sql_condition"] for cl in comp_dict["comparison_levels"]]

            cl_invalid = self.validate_columns_in_sql_strings(
                sql_strings=sql_strings,
                checks=(
                    # Check column validity and for invalid suffixes
                    self.clean_and_return_missing_columns,
                    self.validate_column_suffixes,
                ),
            )
            if cl_invalid:
                invalid_col_tracker.append(
                    (comp_dict.get("output_column_name"), cl_invalid)
                )

        return invalid_col_tracker

    def construct_generic_settings_log_string(self, constructor_dict) -> str:
        if not constructor_dict:
            return ""

        settings_id, InvCols = constructor_dict
        output_warning = [
            "======================================",
            f"Setting: `{settings_id}`",
            "======================================\n",
        ]

        # The blank string acts as a newline
        output_warning.extend([InvCols.construct_log_string(), ""])
        logger.warning("\n".join(output_warning))

    def log_invalid_warnings_within_sql(
        self, invalid_sql_statements: dict[str, list[InvalidCols]]
    ) -> str:
        log_str = []
        for sql, invalid_cols in invalid_sql_statements.items():
            log_str.append(f"    SQL: `{sql}`")

            for c in invalid_cols:
                log_str.append(f"{c.construct_log_string()}")
            # Acts as a newline as we're joining at the end of the str
            log_str.append("")

        return "\n".join(log_str)

    def construct_blocking_rule_log_strings(self, invalid_brs):
        if not invalid_brs:
            return ""

        # `invalid_brs` are in the format of:
        # {
        # "blocking_rule_1": {
        #  - InvalidCols tuple for invalid columns
        #  - InvalidCols tuple for invalid table names
        # }
        # }
        output_warning = [
            "======================================",
            "Invalid Columns(s) in Blocking Rule(s)",
            "======================================\n",
        ]

        output_warning.append(self.log_invalid_warnings_within_sql(invalid_brs))
        logger.warning("\n".join(output_warning))

    def construct_comparison_level_log_strings(self, invalid_cls) -> str:
        if not invalid_cls:
            return ""

        # `invalid_cls` is made up of a tuple containing:
        # 1) The `output_column_name` for the level, if it exists
        # 2) A dictionary in the same format as our blocking rules
        # {sql: [InvalidCols tuples]}

        output_warning = [
            "======================================",
            "Invalid Columns(s) in Comparison(s)",
            "======================================\n",
        ]

        for cn, cls in invalid_cls:
            # Annoyingly, `output_comparison_name` can be None,
            # so this allows those entries without a name to pass
            # through.
            if cn is not None:
                output_warning.append(f"Comparison: {cn}")
            output_warning.append("--------------------------------------")

            output_warning.append(self.log_invalid_warnings_within_sql(cls))

        logger.warning("\n".join(output_warning))

    def construct_output_logs(self):
        # if no errors exist, then we can exit early
        if not self.invalid_cols_detected:
            return

        logger.warning(
            "SETTINGS VALIDATION: Errors were identified in your "
            "settings dictionary. \n"
        )

        self.construct_generic_settings_log_string(self.valid_uid)
        self.construct_generic_settings_log_string(self.valid_cols_to_retain)
        self.construct_blocking_rule_log_strings(self.invalid_brs)
        self.construct_comparison_level_log_strings(self.invalid_cls)

        logger.warning(
            "You may want to verify your settings dictionary has "
            "valid inputs in all fields before continuing."
        )


class InvalidCols(NamedTuple):
    """
    A simple NamedTuple to aid in the construction of
    our log strings.

    It takes in two arguments:
        invalid_type (str): The type of invalid column
            detected. This can be one of: `invalid_cols`,
            `invalid_table_pref` or `invalid_col_suffix`.
        invalid_columns (list): A list of the invalid
            columns that have been detected.
    """

    invalid_type: str
    invalid_columns: list

    @property
    def contains_invalid_columns(self):
        # Quick check to see whether invalid cols exist.
        # Makes list comprehension simpler.
        return True if len(self.invalid_columns) > 0 else False

    @property
    def columns_as_text(self):
        """Returns the invalid columns as a comma-separated
        string wrapped with backticks."""

        return ", ".join(f"`{c}`" for c in self.invalid_columns)

    @property
    def invalid_cols(self):
        return (
            "       - Missing column(s) from input dataframe(s): "
            f"{self.columns_as_text}"
        )

    @property
    def invalid_table_pref(self):
        return (
            "       - Invalid table prefixes (only `l.` and `r.` are valid): "
            f"{self.columns_as_text}"
        )

    @property
    def invalid_col_suffix(self):
        return (
            "       - Invalid table suffixes (only `_l` and `_r` are valid): "
            f"{self.columns_as_text}"
        )

    def construct_log_string(self):
        if self.invalid_columns:
            # calls invalid_cols, invalid_table_pref, etc
            return getattr(self, self.invalid_type)

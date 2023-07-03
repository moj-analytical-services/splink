from __future__ import annotations

import logging
import re
from copy import deepcopy
from functools import reduce
from operator import and_
from typing import NamedTuple

import sqlglot
import sqlglot.expressions as exp

from .input_column import InputColumn, remove_quotes_from_identifiers
from .misc import colour, ensure_is_list

logger = logging.getLogger(__name__)


def remove_suffix(c):
    return re.sub("_[l|r]{1}$", "", c)


class InvalidCols(NamedTuple):
    """
    A simple NamedTuple to aid in the construction of
    our log strings.

    It takes in two arguments:
        invalid_type (str): The type of invalid column
            detected. This can be one of invalid_cols,
            invalid_table_pref or invalid_col_suffix.
        invalid_columns (list): A list of the invalid
            columns that have been detected.
    """

    invalid_type: str
    invalid_columns: list

    @property
    def is_valid(self):
        # Quick check to see whether invalid cols exist.
        # Makes list comprehension simpler.
        return True if len(self.invalid_columns) > 0 else False

    @property
    def _columns(self):
        return "columns" if len(self.invalid_columns) > 1 else "column"

    @property
    def columns_as_text(self):
        return ", ".join(
            f"{colour.ITALICS}`{c}`{colour.END}" for c in self.invalid_columns
        )

    @property
    def invalid_cols(self):
        _c = self._columns
        _is_are = "are" if _c == "columns" else "is"
        return (
            f"The following {_c} {_is_are} missing from one or more "
            f"of your input dataframe(s):\n{self.columns_as_text}"
        )

    @property
    def invalid_table_pref_intro_text(self):
        _c = self._columns
        cont = "contain" if _c == "columns" else "contains"
        return f"The following {_c} {cont} invalid "

    @property
    def invalid_table_pref(self):
        return (
            f"{self.invalid_table_pref_intro_text}\n"
            "table prefixes (only `l.` and `r.` are valid):"
            f"\n{self.columns_as_text}"
        )

    @property
    def invalid_col_suffix(self):
        return (
            f"{self.invalid_table_pref_intro_text}\n"
            "table suffixes (only `_l` and `_r` are valid):"
            f"\n{self.columns_as_text}"
        )

    @property
    def construct_log_string(self):
        if self.invalid_columns:
            # calls invalid_cols, invalid_table_pref, etc
            return getattr(self, self.invalid_type)


class SettingsValidator:
    def __init__(self, linker):
        self.linker = linker
        self.validation_dict = {}

    @property
    def _sql_dialect(self):
        if self.linker._settings_obj:
            return self.linker._settings_obj._sql_dialect
        else:
            return None

    @property
    def cols_to_retain(self):
        return self.clean_list_of_column_names(
            self.linker._settings_obj._additional_cols_to_retain
        )

    @property
    def uid(self):
        uid_as_tree = InputColumn(self.linker._settings_obj._unique_id_column_name)
        return self.clean_list_of_column_names(uid_as_tree)

    @property
    def blocking_rules(self):
        brs = self.linker._settings_obj._blocking_rules_to_generate_predictions
        return [br.blocking_rule for br in brs]

    @property
    def comparisons(self):
        return self.linker._settings_obj.comparisons

    def _validate_dialect(self):
        settings_dialect = self.linker._settings_obj._sql_dialect
        linker_dialect = self.linker._sql_dialect
        if settings_dialect != linker_dialect:
            linker_type = self.linker.__class__.__name__
            raise ValueError(
                f"Incompatible SQL dialect! `settings` dictionary uses "
                f"dialect {settings_dialect}, but expecting "
                f"'{linker_dialect}' for Linker of type `{linker_type}`"
            )

    @property
    def input_columns_by_df(self):
        """A dictionary containing all input dataframes and the columns located
        within.

        Returns:
            dict: A dictionary of the format `{"table_name": [col1, col2, ...]}
        """
        # For each input dataframe, grab the column names and create a dictionary
        # of the form: {table_name: [column_1, column_2, ...]}
        input_columns = {
            k: self.clean_list_of_column_names(v.columns)
            for k, v in self.linker._input_tables_dict.items()
        }

        return input_columns

    @property
    def input_columns(self):
        """
        Returns:
            set: The set intersection of all columns contained within each dataset.
        """
        return reduce(and_, self.input_columns_by_df.values())

    def clean_list_of_column_names(self, col_list, as_tree=True):
        """Clean a list of columns names by removing the quote characters
        that may exist.

        Args:
            col_list (list): A list of columns names.
            as_tree (bool): Whether the input columns are already SQL
                trees or need conversions.

        Returns:
            set: A set of column names without quotes.
        """
        if col_list is None:
            return ()  # needs to be a blank iterable

        col_list = ensure_is_list(col_list)
        if as_tree:
            col_list = [c.input_name_as_tree for c in col_list]
        return set(remove_quotes_from_identifiers(tree).sql() for tree in col_list)

    def remove_prefix_and_suffix_from_column(
        self, col_syntax_tree: sqlglot.expressions
    ):
        """Remove the prefix and suffix from a given sqlglot syntax tree
        and return it as a string of SQL.

        Args:
            col_syntax_tree (sqlglot.expressions): _description_

        Returns:
            str: A column without `l.` or `_l`
        """
        col_syntax_tree.args["table"] = None
        return remove_suffix(col_syntax_tree.sql())

    def check_column_exists(self, column_name: str):
        """Check whether a column name exists within all of the input
        dataframes.

        Args:
            column_name (str): A column name to check

        Returns:
            bool: True if the column exists.
        """
        return column_name in self.input_columns

    def return_missing_columns(self, cols_to_check: set) -> list:
        """
        Args:
            cols_to_check (set): A list of columns to check for the
                existence of.

        Returns:
            list: Returns a list of all input columns not found within
                your raw input tables.
        """
        missing_cols = [
            col for col in cols_to_check if not self.check_column_exists(col)
        ]
        return InvalidCols("invalid_cols", missing_cols)

    def clean_and_return_missing_columns(self, cols: list[sqlglot.expressions]):
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
        cols = set(self.remove_prefix_and_suffix_from_column(c) for c in cols)
        return self.return_missing_columns(cols)

    def validate_table_name(self, col: sqlglot.expressions):
        """Check if the table name supplied is valid.

        Args:
            col (sqlglot.expressions): A column string given as
                a sqlglot expression

        Returns:
            bool: Whether the table name exists and is valid.
        """
        table_name = col.table
        # If the table name exists, check it's valid.
        return table_name not in ["l", "r"]

    def validate_table_names(self, cols: list[sqlglot.expressions]):
        """Validate a series of table names with `validate_table_name`

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
        invalid_cols = [c.sql() for c in cols if self.validate_table_name(c)]
        return InvalidCols(invalid_type, invalid_cols)

    def validate_column_suffix(self, col: sqlglot.expressions):
        """Check if the column suffix supplied is valid.

        Args:
            col (sqlglot.expressions): A column string given as
                a sqlglot expression

        Returns:
            bool: Whether a valid column suffix exists
        """
        # Check if the supplied col suffix is valid.
        return not col.sql().endswith(("_l", "_r"))

    def validate_column_suffixes(self, cols: list[sqlglot.expressions]):
        """Validate a series of column suffixes with `validate_column_suffix`

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
        invalid_cols = [c.sql() for c in cols if self.validate_column_suffix(c)]
        return InvalidCols(invalid_type, invalid_cols)

    def validate_columns_in_sql_string(
        self,
        sql_string: str,
        checks: list,
    ):
        """Evaluate whether the columns supplied in a given string of SQL
        exist in our raw data.

        Args:
            sql_string (str): A string of valid SQL
            checks (list[function]): The functions used to check the parsed
                sql string. These can be: `clean_and_return_missing_columns`,
                `validate_table_names` and `validate_column_suffixes`

        Returns:
            list[InvalidCols]: A list of InvalidCols classes, denoting the
                the `invalid_type` and a list of the invalid columns that were
                identified.
        """

        try:
            syntax_tree = sqlglot.parse_one(sql_string, read=self._sql_dialect)
        except Exception:
            # Usually for the `ELSE` clause. If we can't parse a
            # SQL condition, it's better to just pass.
            return

        cols = [
            remove_quotes_from_identifiers(col)
            for col in syntax_tree.find_all(exp.Column)
        ]
        # deepcopy to ensure our syntax trees aren't manipulated
        invalid_tree = [check(deepcopy(cols)) for check in checks]
        # Report only those checks with valid flags (i.e. there's an invalid
        # column in one of the checks)
        return [tree for tree in invalid_tree if tree.is_valid]

    def validate_columns_in_sql_string_dict_comp(
        self,
        sql_conds,
        prefix_suffix_fun,
    ):
        # This is simply a wrapper function that loops
        # around validate_columns_in_sql_string so we can
        # more easily reuse the code
        def validate(
            sql,
        ):
            return self.validate_columns_in_sql_string(
                sql,
                checks=(
                    self.clean_and_return_missing_columns,
                    prefix_suffix_fun,
                ),
            )

        return {sql: validate(sql) for sql in sql_conds if validate(sql)}

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
        # The `is_valid` check simply tests to see if any values have
        # been flagged. If there are no invalid cols, return None.
        if missing_cols.is_valid:
            return settings_id, missing_cols

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
        # See docstring for `validate_columns_in_sql_string_dict_comp`.
        return self.validate_columns_in_sql_string_dict_comp(
            sql_conds=self.blocking_rules,
            prefix_suffix_fun=self.validate_table_names,
        )

    @property
    def validate_comparison_levels(self):
        # See docstring for `validate_columns_in_sql_string_dict_comp`.
        invalid_col_tracker = []
        for comparisons in self.comparisons:
            # pull out comparison dict
            comp_dict = comparisons._comparison_dict
            sql_conds = [cl["sql_condition"] for cl in comp_dict["comparison_levels"]]

            cl_invalid = self.validate_columns_in_sql_string_dict_comp(
                sql_conds=sql_conds,
                prefix_suffix_fun=self.validate_column_suffixes,
            )
            if cl_invalid:
                output_c_name = comp_dict.get("output_column_name")
                # This needs to be a tuple as output_c_name can be
                # set to None.
                output_tuple = (
                    output_c_name,
                    cl_invalid,
                )
                invalid_col_tracker.append(output_tuple)

        return invalid_col_tracker


class InvalidSettingsLogger(SettingsValidator):
    def __init__(self, linker):
        self.settings_validator = super().__init__(linker)
        self.bold_underline = colour.BOLD + colour.UNDERLINE
        self.bold_red = colour.BOLD + colour.RED

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

    def construct_generic_settings_log_string(self, constructor_dict):
        settings_id, InvCols = constructor_dict
        logger.warning(
            f"{colour.BOLD}A problem was found within your setting "
            f"`{settings_id}`:{colour.END}\n{InvCols.construct_log_string}"
            "\n"
        )

    def log_invalid_warnings_within_sql(self, invalid_sql_statements):
        for sql, invalid_cols in invalid_sql_statements.items():
            invalid_strings = "\n".join(c.construct_log_string for c in invalid_cols)
            sql = f"{self.bold_red}`{sql}`{colour.END}"
            logger.warning(f"{sql}:\n{invalid_strings}")

    def construct_blocking_rule_log_strings(self, invalid_brs):
        # `invalid_brs` are in the format of:
        # {
        # "blocking_rule_1": {
        #  - InvalidCols tuple for invalid columns
        #  - InvalidCols tuple for invalid table names
        # }
        # }

        logger.warning(
            f"{colour.BOLD}The following blocking rule(s) were "
            f"found to contain invalid column(s):{colour.END}"
        )

        self.log_invalid_warnings_within_sql(invalid_brs)

        logger.warning("\n")

    def construct_comparison_level_log_strings(self, invalid_cls):
        # `invalid_cls` is made up of a tuple containing:
        # 1) The `output_column_name` for the level, if it exists
        # 2) A dictionary in the same format as our blocking rules
        # {sql: [InvalidCols tuples]}
        logger.warning(
            f"{colour.BOLD}The following comparison(s) were "
            f"found to contain invalid column(s):{colour.END}"
        )
        for cn, cls in invalid_cls:
            # Annoyingly, `output_comparison_name` can be None,
            # so this allows those entries without a name to pass
            # through.
            if cn is not None:
                logger.warning(f"{colour.BOLD}{cn}{colour.END}")
            self.log_invalid_warnings_within_sql(cls)

        logger.warning("\n")

    def construct_output_logs(self):
        # if no errors exist, return
        if not self.invalid_cols_detected:
            return

        if self.valid_uid:
            self.construct_generic_settings_log_string(self.valid_uid)

        if self.valid_cols_to_retain:
            self.construct_generic_settings_log_string(self.valid_cols_to_retain)

        if self.invalid_brs:
            self.construct_blocking_rule_log_strings(self.invalid_brs)

        if self.invalid_cls:
            self.construct_comparison_level_log_strings(self.invalid_cls)

        # Only trigger if one of the outputs is valid
        logger.warning(
            f"{self.bold_underline}"
            "You may want to verify your settings dictionary has "
            "valid inputs in all fields before continuing."
            f"{colour.END}"
        )

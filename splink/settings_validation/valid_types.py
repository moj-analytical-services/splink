from __future__ import annotations

import logging
import sys

from ..comparison import Comparison
from ..comparison_level import ComparisonLevel
from ..exceptions import (
    ComparisonSettingsException,
    ErrorLogger,
    InvalidDialect,
    SplinkException,
)
from ..logging_messages import (
    _comparison_dict_missing_comparison_level_key_log_str,
    _invalid_comparison_data_type_log_str,
    _invalid_comparison_level_used_log_str,
    _invalid_dialects_log_str,
    _single_comparison_log_str,
    _single_dataframe_log_str,
)
from .settings_validator import DataFrameColumns

logger = logging.getLogger(__name__)


def extract_sql_dialect_from_cll(cll):
    if isinstance(cll, dict):
        return cll.get("sql_dialect")
    else:
        return getattr(cll, "_sql_dialect", None)


def pretty_comparison_name(comparison_dict):
    if isinstance(comparison_dict, Comparison):
        return (
            f"<{comparison_dict._comparison_description} for column "
            f"'{comparison_dict._output_column_name}'>"
        )
    else:
        return f"{str(comparison_dict)[:75]}... "


class InvalidTypesAndValuesLogger:

    """Most types are checked within the settings schema validation step -
    https://github.com/moj-analytical-services/splink/blob/master/splink/validate_jsonschema.py.

    For any types that can't be checked in this step, run some quick validation checks.
    """

    def __init__(self, linker):
        self.linker = linker

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


def evaluate_comparison(
    comparison_dict: [Comparison, dict], comp_str: str
) -> Exception:
    """
    Given a comparison_dict, evaluate the comparison is valid.
    If it's invalid, queue up an error.

    This can then be logged with `ErrorLogger.raise_and_log_all_errors`
    or raised instantly as an error.
    """

    if not isinstance(comparison_dict, (Comparison, dict)):
        if isinstance(comparison_dict, ComparisonLevel):
            log = _invalid_comparison_level_used_log_str(comp_str)
        else:
            log = _invalid_comparison_data_type_log_str(comp_str)

        return TypeError(log)
    else:
        if isinstance(comparison_dict, Comparison):
            comparison_dict = comparison_dict.as_dict()

        # missing the comparison_levels key
        if comparison_dict.get("comparison_levels") is None:
            return SyntaxError(
                _comparison_dict_missing_comparison_level_key_log_str(comp_str)
            )


def evaluate_comparison_dialects(
    comparison_dict: [Comparison, dict], sql_dialect: str, comp_str: str
) -> InvalidDialect:
    """
    Given a comparison_dict, assess whether the sql dialect is valid for
    your selected linker.

    This function is aimed at preventing invalid imports when users are
    working across multiple linker dialects.

    This function is aiming to flag the following behaviour:
    ```
    from splink.duckdb.comparison_library import exact_match
    from splink.spark.linker import SparkLiner

    settings = {
        ...,
        comparisons = [
            ...,
            exact_match("dob")  # em taken from duckdb library
        ]
    }

    SparkLinker(df, settings)  # errors due to mismatched CL imports
    ```
    """

    # Ensure the comparison_dict is either a Comparison or a dict
    if not isinstance(comparison_dict, (Comparison, dict)):
        return

    # Skip validation if sql_dialect is None or comparison_dict is a dict
    if sql_dialect is None:
        return

    # Extract the comparison dialects based on the comparison_dict type
    if isinstance(comparison_dict, Comparison):
        comparison_dialects = [
            extract_sql_dialect_from_cll(comp_level)
            for comp_level in comparison_dict.comparison_levels
        ]
    else:
        comparison_dialects = [
            extract_sql_dialect_from_cll(comp_level)
            for comp_level in comparison_dict.get("comparison_levels", [])
        ]

    comparison_dialects = set(comparison_dialects)
    # if no dialect has been supplied, ignore it
    comparison_dialects.discard(None)

    comparison_dialects = [
        dialect for dialect in comparison_dialects if sql_dialect != dialect
    ]  # sort only needed to ensure our tests pass

    if comparison_dialects:
        comparison_dialects.sort()
        dialects = ", ".join(f"'{c}'" for c in comparison_dialects)
        return InvalidDialect(_invalid_dialects_log_str(comp_str, dialects))


def _check_input_dataframes_have_only_one_comparison_column(
    settings_dict: dict, input_columns: DataFrameColumns
) -> Exception:
    # Loop and exit if any dataframe has more than one potential
    # linkage column. Here, we loop through all of our dfs to ensure we
    # capture cases in which identical columns are named differently across
    # dfs.

    source_dataset = settings_dict.get("source_dataset_column_name", "source_dataset")
    uid = settings_dict.get("unique_id_column_name", "unique_id")
    required_cols = (source_dataset, uid)

    # input_columns - a hashmap w/ each dataframe and its corresponding columns
    for df_cols in input_columns._input_columns_by_df.values():
        # Pop UID and source dataset columns if present
        if len(df_cols.difference(required_cols)) > 1:
            return

    return _single_dataframe_log_str()


def validate_comparison_levels(
    error_logger: ErrorLogger, comparisons: list, linker_dialect: str
) -> ErrorLogger:
    """Takes in a list of comparisons from your settings
    object and evaluates whether:
    1) It is of a valid type (ComparisonLevel or Dict).
    2) It contains valid dictionary key(s).

    Args:
        comparisons (list): Your comparisons, as outlined in
            `settings["comparisons"]`.
    """

    # Extract problematic comparisons
    for c_dict in comparisons:
        # Error logger's append ignores NULLs, so
        # if no error is found, append won't do anything.
        comp_str = pretty_comparison_name(c_dict)
        error_logger.append(
            [
                evaluate_comparison(c_dict, comp_str),
                evaluate_comparison_dialects(c_dict, linker_dialect, comp_str),
            ]
        )

    return error_logger


def validate_input_comparison_dataframe_size(linker, settings_dict: dict) -> Exception:
    # Check if the input dataframe/comparisons are invalid -
    # https://github.com/moj-analytical-services/splink/issues/1362

    if hasattr(sys, "_called_from_test"):
        return

    errors = []
    df_columns = DataFrameColumns(linker)
    comparisons = settings_dict.get("comparisons", [])

    # Warn the user that they've entered too few comparison arguments.
    # "_called_from_test" is a global var set in our tests
    if len(comparisons) == 1:
        errors.append(_single_comparison_log_str())

    # Check if there's only one linkage column that exists.
    # Returns None if more than one is found.
    invalid_input_dfs = _check_input_dataframes_have_only_one_comparison_column(
        settings_dict, df_columns
    )
    if invalid_input_dfs:
        errors.append(invalid_input_dfs)

    if errors:
        link_to_docs = "Please see: https://github.com/moj-analytical-services/splink#what-data-does-splink-work-best-with for more info"  # noqa
        errors.append(link_to_docs)

        raise SplinkException("\n".join(errors))


def log_comparison_errors(settings_dict: dict) -> None:
    """
    Log any errors arising from `validate_comparison_levels`.
    """

    comparisons = settings_dict.get("comparisons")
    sql_dialect = settings_dict.get("sql_dialect")

    # Check for empty inputs - Expecting None or []
    # We allow users to submit empty (or close to empty) settings
    # dictionaries.
    if not comparisons:
        return

    error_logger = ErrorLogger()
    error_logger = validate_comparison_levels(error_logger, comparisons, sql_dialect)

    # Raise and log any errors identified
    plural_this = "this" if len(error_logger.raw_errors) == 1 else "these"
    comp_hyperlink_txt = (
        "\n\n"
        f"For more information on {plural_this} error, please visit:\n"
        "https://moj-analytical-services.github.io/splink/topic_guides/comparisons/customising_comparisons.html"
    )

    error_logger.raise_and_log_all_errors(
        exception=ComparisonSettingsException, additional_txt=comp_hyperlink_txt
    )

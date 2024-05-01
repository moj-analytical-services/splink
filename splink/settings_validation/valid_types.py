from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Union

from ..comparison import Comparison
from ..comparison_level import ComparisonLevel
from ..default_from_jsonschema import default_value_from_schema
from ..exceptions import (
    ComparisonSettingsException,
    ErrorLogger,
    InvalidDialect,
    InvalidSplinkInput,
)
from .settings_column_cleaner import clean_user_input_columns
from .settings_validation_log_strings import (
    construct_single_dataframe_log_str,
    create_incorrect_dialect_import_log_string,
    create_invalid_comparison_level_log_string,
    create_invalid_comparison_log_string,
    create_no_comparison_levels_error_log_string,
)

if TYPE_CHECKING:
    from ..splink_dataframe import SplinkDataFrame


logger = logging.getLogger(__name__)


def extract_sql_dialect_from_cll(cll):
    if isinstance(cll, dict):
        return cll.get("sql_dialect")
    else:
        return getattr(cll, "_sql_dialect", None)


def _validate_dialect(settings_dialect: str, linker_dialect: str, linker_type: str):
    if settings_dialect != linker_dialect:
        raise ValueError(
            f"Incompatible SQL dialect! `settings` dictionary uses "
            f"dialect {settings_dialect}, but expecting "
            f"'{linker_dialect}' for Linker of type `{linker_type}`"
        )


def _check_input_dataframes_for_single_comparison_column(
    input_columns: Dict[str, "SplinkDataFrame"],
    source_dataset_column_name: str = None,
    unique_id_column_name: str = None,
):
    if source_dataset_column_name is None:
        source_dataset_column_name = default_value_from_schema(
            "source_dataset_column_name", "root"
        )
    if unique_id_column_name is None:
        unique_id_column_name = default_value_from_schema(
            "unique_id_column_name", "root"
        )

    input_columns = clean_user_input_columns(
        input_columns.items(), return_as_single_column=False
    )

    required_cols = (source_dataset_column_name, unique_id_column_name)

    # Loop and exit if any dataframe has only possible comparison column
    for columns in input_columns.values():
        unique_columns = set(columns) - set(required_cols)

        if len(unique_columns) == 1:
            raise InvalidSplinkInput(construct_single_dataframe_log_str(input_columns))


def validate_comparison_levels(
    error_logger: ErrorLogger, comparisons: list, linker_dialect: str
):
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
        # If no error is found, append won't do anything
        error_logger.log_error(evaluate_comparison_dtype_and_contents(c_dict))
        error_logger.log_error(
            check_comparison_imported_for_correct_dialect(c_dict, linker_dialect)
        )

    return error_logger


def check_comparison_level_types(
    comparison_levels: Union[Comparison, Dict], comparison_str: str
):
    """
    Given a comparison, check all of its contents are either a dictionary
    or a ComparisonLevel.
    """

    # Error to be handled in another function
    if len(comparison_levels) == 0:
        return

    # Loop through our CLs and check their types. Report any invalid types to the user.
    invalid_levels = []
    for comp_level in comparison_levels:
        if not isinstance(comp_level, (ComparisonLevel, dict)):
            cl_str = f"{str(comp_level)[:50]}... "
            invalid_levels.append((cl_str, type(comp_level)))

    if invalid_levels:
        error_message = create_invalid_comparison_level_log_string(
            comparison_str, invalid_levels
        )
        return TypeError(error_message)


def evaluate_comparison_dtype_and_contents(comparison_dict):
    """
    Given a Comparison class or a comparison_dict, check that the comparison
    is of a valid type and contains the required contents.

    Checks include:
        - Is the comparison a Comparison class, a dict or other?
        - Does the comparison contain any comparison levels?
        - Are the comparison levels contained in the comparison all valid?

    Any identified errors are subsequently passed to the error Logger.
    """

    comp_str = f"{str(comparison_dict)[:65]}... "

    if not isinstance(comparison_dict, (Comparison, dict)):
        log_str = create_invalid_comparison_log_string(
            comp_str, comparison_level=isinstance(comparison_dict, ComparisonLevel)
        )
        return TypeError(log_str)

    if isinstance(comparison_dict, Comparison):
        comparison_dict = comparison_dict.as_dict()

    comp_levels = comparison_dict.get("comparison_levels")

    if comp_levels is None:
        return SyntaxError(create_no_comparison_levels_error_log_string(comp_str))

    # Check comparisons
    return check_comparison_level_types(comp_levels, comp_str)


def check_comparison_imported_for_correct_dialect(comparison_dict, sql_dialect):
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

    if not sql_dialect or not isinstance(comparison_dict, (Comparison, dict)):
        return

    comp_str = f"{str(comparison_dict)[:65]}... "
    comparison_levels = (
        comparison_dict.comparison_levels
        if isinstance(comparison_dict, Comparison)
        else comparison_dict.get("comparison_levels", [])
    )

    comparison_dialects = set(
        extract_sql_dialect_from_cll(cl) for cl in comparison_levels
    )
    comparison_dialects.discard(None)

    # Filter out dialects that match the expected sql_dialect
    invalid_dialects = [
        dialect for dialect in comparison_dialects if dialect != sql_dialect
    ]

    if invalid_dialects:
        error_message = create_incorrect_dialect_import_log_string(
            comp_str, sorted(invalid_dialects)
        )
        return InvalidDialect(error_message)


def _log_comparison_errors(comparisons: List[Comparison], linker_dialect: str):
    """
    Log any errors arising from various comparison validation checks.
    """

    # Check for empty inputs - Expecting None or []
    if not comparisons:
        return

    error_logger = ErrorLogger()

    error_logger = validate_comparison_levels(error_logger, comparisons, linker_dialect)

    # Raise and log any errors identified
    plural_this = "this" if len(error_logger.raw_errors) == 1 else "these"
    comp_hyperlink_txt = (
        f"\nFor more info on how to construct comparisons and avoid {plural_this} "
        "error, please visit:\n"
        "https://moj-analytical-services.github.io/splink/topic_guides/comparisons/customising_comparisons.html"
    )

    error_logger.raise_and_log_all_errors(
        exception=ComparisonSettingsException, additional_txt=comp_hyperlink_txt
    )

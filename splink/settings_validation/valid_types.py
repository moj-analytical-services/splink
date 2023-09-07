from __future__ import annotations

import logging

from ..comparison import Comparison
from ..comparison_level import ComparisonLevel
from ..exceptions import ComparisonSettingsException, ErrorLogger
from .settings_validator import SettingsValidator

logger = logging.getLogger(__name__)


class InvalidTypesAndValuesLogger(SettingsValidator):

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


def validate_comparison_levels(comparisons: list):
    """Takes in a list of comparisons from your settings
    object and evaluates whether:
    1) It is of a valid type (ComparisonLevel or Dict).
    2) It contains valid dictionary key(s).

    Args:
        comparisons (list): Your comparisons, as outlined in
            `settings["comparisons"]`.
    """

    # Extract problematic comparisons
    comp_error_logger = ErrorLogger()  # logger to track all identified errors
    for c_dict in comparisons:
        eval_dtype = evaluate_comparison_dtype_and_contents(c_dict)
        # If no error is found, append won't do anything
        comp_error_logger.append(eval_dtype)

    return comp_error_logger


def log_comparison_errors(comparisons):
    """
    Log any errors arising from `validate_comparison_levels`.
    """
    error_logger = validate_comparison_levels(comparisons)

    # Raise and log any errors identified
    comp_hyperlink_txt = """
    For more info on this error, please visit:
    https://moj-analytical-services.github.io/splink/topic_guides/comparisons/customising_comparisons.html
    """

    error_logger.raise_and_log_all_errors(
        exception=ComparisonSettingsException, additional_txt=comp_hyperlink_txt
    )


def evaluate_comparison_dtype_and_contents(comparison_dict):
    """
    Given a comparison_dict, evaluate the comparison is valid.
    If it's invalid, queue up an error.

    This can then be logged with `ErrorLogger.raise_and_log_all_errors`
    or raised instantly as an error.
    """

    comp_str = f"{str(comparison_dict)[:30]}... "

    if not isinstance(comparison_dict, (Comparison, dict)):

        if isinstance(comparison_dict, ComparisonLevel):
            return TypeError(
                f"""
            {comp_str} is a comparison level and
            cannot be used as a standalone comparison.
            """
            )
        else:
            return TypeError(
                f"""
            `{comp_str}` is an invalid data type.
            Please only include dictionaries or objects of
            the `Comparison` class.
            """
            )
    else:
        if isinstance(comparison_dict, Comparison):
            comparison_dict = comparison_dict.as_dict()
        comp_level = comparison_dict.get("comparison_levels")

        if comp_level is None:
            return SyntaxError(
                f"""
                {comp_str} is missing the required `comparison_levels`
                key. Please ensure you include this in all comparisons
                used in your settings object.
                """
            )

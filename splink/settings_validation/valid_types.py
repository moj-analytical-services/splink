from __future__ import annotations

import logging

from ..comparison import Comparison
from ..comparison_level import ComparisonLevel
from ..exceptions import ComparisonSettingsException, ErrorLogger, InvalidDialect

logger = logging.getLogger(__name__)


def extract_sql_dialect_from_cll(cll):
    if isinstance(cll, dict):
        return cll.get("sql_dialect")
    else:
        return getattr(cll, "_sql_dialect", None)


def _validate_dialect(settings_dialect: str, linker_dialect: str, linker_type: str):
    # settings_dialect = self.linker._settings_obj._sql_dialect
    # linker_dialect = self.linker._sql_dialect
    # linker_type = self.linker.__class__.__name__
    if settings_dialect != linker_dialect:
        raise ValueError(
            f"Incompatible SQL dialect! `settings` dictionary uses "
            f"dialect {settings_dialect}, but expecting "
            f"'{linker_dialect}' for Linker of type `{linker_type}`"
        )


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
        error_logger.append(evaluate_comparison_dtype_and_contents(c_dict))
        error_logger.append(evaluate_comparison_dialects(c_dict, linker_dialect))

    return error_logger


def log_comparison_errors(comparisons, linker_dialect):
    """
    Log any errors arising from `validate_comparison_levels`.
    """

    # Check for empty inputs - Expecting None or []
    if not comparisons:
        return

    error_logger = ErrorLogger()

    error_logger = validate_comparison_levels(error_logger, comparisons, linker_dialect)

    # Raise and log any errors identified
    plural_this = "this" if len(error_logger.raw_errors) == 1 else "these"
    comp_hyperlink_txt = f"""
    For more info on {plural_this} error, please visit:
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

    comp_str = f"{str(comparison_dict)[:65]}... "

    if not isinstance(comparison_dict, (Comparison, dict)):
        if isinstance(comparison_dict, ComparisonLevel):
            return TypeError(
                f"""
            {comp_str}
            is a comparison level and
            cannot be used as a standalone comparison.
            """
            )
        else:
            return TypeError(
                f"""
            The comparison level `{comp_str}`
            is of an invalid data type.
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
            {comp_str}
            is missing the required `comparison_levels` dictionary
            key. Please ensure you include this in all comparisons
            used in your settings object.
            """
            )


def evaluate_comparison_dialects(comparison_dict, sql_dialect):
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
    comp_str = f"{str(comparison_dict)[:65]}... "

    # This function doesn't currently support dictionary comparisons
    # as it's assumed users won't submit the sql dialect when using
    # that functionality.

    # All other scenarios will be captured by
    # `evaluate_comparison_dtype_and_contents` and can be skipped.
    # Where sql_dialect is empty, we also can't verify the CLs.
    if not isinstance(comparison_dict, (Comparison, dict)) or sql_dialect is None:
        return

    # Alternatively, if we just want to check where the import's origin,
    # we could evalute the import signature using:
    # `comparison_dict.__class__`
    if isinstance(comparison_dict, Comparison):
        comparison_dialects = set(
            [
                extract_sql_dialect_from_cll(comp_level)
                for comp_level in comparison_dict.comparison_levels
            ]
        )
    else:  # only other value here is a dict
        comparison_dialects = set(
            [
                extract_sql_dialect_from_cll(comp_level)
                for comp_level in comparison_dict.get("comparison_levels", [])
            ]
        )
    # if no dialect has been supplied, ignore it
    comparison_dialects.discard(None)

    comparison_dialects = [
        dialect for dialect in comparison_dialects if sql_dialect != dialect
    ]  # sort only needed to ensure our tests pass
    # comparison_dialects = [
    #     dialect for dialect in comparison_dialects if sql_dialect != dialect
    # ].sort()  # sort only needed to ensure our tests pass

    if comparison_dialects:
        comparison_dialects.sort()
        return InvalidDialect(
            f"""
            {comp_str}
            contains the following invalid SQL dialect(s)
            within its comparison levels - {', '.join(comparison_dialects)}.
            Please ensure that you're importing comparisons designed
            for your specified linker.
        """
        )

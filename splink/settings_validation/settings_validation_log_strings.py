from functools import partial
from typing import Dict, List, NamedTuple, Tuple


def indent_error_message(message):
    """Indents an error message by 4 spaces."""
    return "\n    ".join(message.splitlines())


class InvalidColumnsLogGenerator(NamedTuple):
    """
    A simple NamedTuple to aid in the construction of
    our log strings.

    It takes two arguments:
        invalid_type (str): The type of invalid column
            detected. This can be one of: `invalid_cols`,
            `invalid_table_pref` or `invalid_col_suffix`.
        invalid_columns (list): A list of the invalid
            columns that have been detected.
    """

    invalid_type: str
    invalid_columns: set

    @property
    def log_string_prefix(self):
        return "       - "

    @property
    def columns_as_text(self):
        """Returns the invalid columns as a comma-separated
        string wrapped with backticks."""

        return ", ".join(f"`{c}`" for c in self.invalid_columns)

    @property
    def missing_columns(self):
        return "Missing column(s) from input dataframe(s): "

    @property
    def invalid_table_name(self):
        return "Invalid table names provided (only `l.` and `r.` are valid): "

    @property
    def invalid_column_suffix(self):
        return "Invalid table suffixes provided (only `_l` and `_r` are valid): "

    def construct_log_string(self):
        # calls invalid_cols, invalid_table_pref, etc
        invalid_string = getattr(self, self.invalid_type)
        return self.log_string_prefix + invalid_string + self.columns_as_text


# Create a series of partial implementations to make the trackers more explicit
MissingColumnsLogGenerator = partial(InvalidColumnsLogGenerator, "missing_columns")
InvalidTableNamesLogGenerator = partial(
    InvalidColumnsLogGenerator, "invalid_table_name"
)
InvalidColumnSuffixesLogGenerator = partial(
    InvalidColumnsLogGenerator, "invalid_column_suffix"
)


def construct_missing_settings_column_log(constructor_dict) -> str:
    if not constructor_dict:
        return ""

    settings_id, InvalidCols = constructor_dict
    output_warning = [
        "======================================",
        f"Setting: `{settings_id}`",
        "======================================\n",
    ]

    # The blank string acts as a newline
    output_warning.extend([InvalidCols.construct_log_string(), ""])
    return "\n".join(output_warning)


def construct_invalid_sql_log_string(
    # invalid_sql_statements: dict[str, list[InvalidColumnsTracker]]
    invalid_sql_statements,
) -> str:
    log_str = []
    for sql, invalid_cols in invalid_sql_statements.items():
        log_str.append(f"    SQL: `{sql}`")

        for c in invalid_cols:
            log_str.append(f"{c.construct_log_string()}")
        # Acts as a newline as we're joining at the end of the str
        log_str.append("")

    return "\n".join(log_str)


def construct_missing_column_in_blocking_rule_log(invalid_brs):
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

    output_warning.append(construct_invalid_sql_log_string(invalid_brs))
    return "\n".join(output_warning)


def construct_missing_column_in_comparison_level_log(invalid_cls) -> str:
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

    for comp_name, comp_lvls in invalid_cls:
        # Annoyingly, `output_comparison_name` can be None,
        # so this allows those entries without a name to pass
        # through.
        if comp_name is not None:
            output_warning.append(f"Comparison: {comp_name}")
        output_warning.append("--------------------------------------")

        output_warning.append(construct_invalid_sql_log_string(comp_lvls))

    return "\n".join(output_warning)


def create_invalid_comparison_level_log_string(
    comparison_string: str, invalid_comparison_levels: List[Tuple[str, str]]
):
    invalid_levels_str = ",\n".join(
        [
            f"- Type: {type_name}. Level: {level}"
            for level, type_name in invalid_comparison_levels
        ]
    )

    log_message = (
        f"\nThe comparison `{comparison_string}` contains the following invalid "
        f"levels:\n{invalid_levels_str}\n\n"
        "Please only include dictionaries or objects of "
        "the `ComparisonLevel` class."
    )

    return indent_error_message(log_message)


def create_invalid_comparison_log_string(
    comparison_string: str, comparison_level: bool
):
    if comparison_level:
        type_msg = "is a comparison level"
    else:
        type_msg = "is of an invalid data type"

    log_message = (
        f"\n{comparison_string}\n"
        f"{type_msg} and must be nested within a comparison.\n"
        "Please only include dictionaries or objects of the `Comparison` class.\n"
    )
    return indent_error_message(log_message)


def create_no_comparison_levels_error_log_string(comparison_string: str):
    log_message = (
        f"\n{comparison_string}\n"
        "is missing the required `comparison_levels` dictionary"
        "key. Please ensure you\ninclude this in all comparisons"
        "used in your settings object.\n"
    )
    return indent_error_message(log_message)


def create_incorrect_dialect_import_log_string(
    comparison_string: str, comparison_dialects: List[str]
):
    log_message = (
        f"\n{comparison_string}\n"
        "contains the following invalid SQL dialect(s)"
        f"within its comparison levels - {', '.join(comparison_dialects)}.\n"
        "Please ensure that you're importing comparisons designed "
        "for your specified linker.\n"
    )
    return indent_error_message(log_message)


def construct_single_dataframe_log_str(input_columns: Dict[str, str]) -> str:
    if len(input_columns) == 1:
        df_txt = "dataframe is"
    else:
        df_txt = "dataframes are"

    log_message = (
        f"\nThe provided {df_txt} unsuitable for linkage with Splink as\n"
        "it contains only a single column for matching.\n"
        "Splink is not designed for linking based on a single 'bag of words'\n"
        "column, such as a table with only a 'company name' column and\n"
        "no other details.\n\nFor more information see: \n"
        "https://github.com/moj-analytical-services/splink/issues/1362"
    )

    return log_message

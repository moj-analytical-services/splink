def execute_sql_logging_message_info(templated_name, physical_name):
    return (
        f"Executing sql to create "
        f"{templated_name} "
        f"as physical name {physical_name}"
    )


def log_sql(sql):
    return "\n------Start SQL---------\n" f"{sql}\n" "-------End SQL-----------\n"


# Error Messages
def _format_log_string(log_str: str) -> str:
    log = "\n    ".join(log_str)
    return f"\n    {log}\n"


def _invalid_dialects_log_str(comparison_str: str, dialects: str) -> str:
    return _format_log_string(
        [
            f"{comparison_str}",
            "contains the following invalid SQL dialect(s) within its",
            f"comparison levels - {dialects}.",
            "Ensure that you are using comparisons designed for your specified linker.",
        ]
    )


def _invalid_comparison_level_used_log_str(comparison_str) -> str:
    return _format_log_string(
        [
            f"{comparison_str}",
            "is a comparison level and cannot be used as a standalone comparison.",
        ]
    )


def _invalid_comparison_data_type_log_str(comparison_str) -> str:
    return _format_log_string(
        [
            f"The comparison `{comparison_str}`",
            "is of an invalid data type.",
            "Please only include dictionaries or objects of the `Comparison` class.",
        ]
    )


def _comparison_dict_missing_comparison_level_key_log_str(comparison_str) -> str:
    return _format_log_string(
        [
            f"{comparison_str} is missing the required `comparison_levels` dictionary",
            "key. Please ensure you include this in all comparisons",
            "used in your settings object.",
        ]
    )


def _single_comparison_log_str() -> str:
    return _format_log_string(
        [
            "Splink requires multiple comparisons to accurately estimate",
            "underlying parameters.",
        ]
    )[
        :-2
    ]  # remove the final "\n"


def _single_dataframe_log_str() -> str:
    return _format_log_string(
        [
            "It appears that your input dataframe(s) contain only a single column",
            "available for linkage. Splink is not designed for linking based on a",
            "single 'bag of words' column, such as a table with only a 'company name'",
            "column and no other details.",
        ]
    )

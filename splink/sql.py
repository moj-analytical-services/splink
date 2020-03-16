import re
import copy

import pyspark.sql.functions as f



def sql_gen_comparison_columns(columns:list) -> str:
    """Build SQL expression that renames columns and sets them aside each other for comparisons

    Args:
        columns (list): [description]

   Examples:
        >>> sql_gen_comparison_columns(["name", "dob"])
        "name_l, name_r, dob_l, dob_r"

    Returns:
        SQL expression
    """

    l = [f"l.{c} as {c}_l" for c in columns]
    r = [f"r.{c} as {c}_r" for c in columns]
    both = zip(l, r)
    flat_list = [item for sublist in both for item in sublist]
    return ", ".join(flat_list)



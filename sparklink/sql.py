import re
import copy

import logging
import pyspark.sql.functions as f

log = logging.getLogger(__name__)
from .logging_utils import format_sql

def comparison_columns_select_expr(df):
    """
    Compare cols in df
    Example output from a input df with columns [first_name,  surname]
    l.first_name as first_name_l, r.first_name as first_name_r, l.surname as surname_l, r.surname as surname_r
    """

    l = [f"l.{c} as {c}_l" for c in df.columns]
    r = [f"r.{c} as {c}_r" for c in df.columns]
    both = zip(l, r)
    flat_list = [item for sublist in both for item in sublist]
    return ", ".join(flat_list)

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


def sql_gen_concat_cols(cols, delimiter=" "):
    """
    Generate a sql expression to concatenate multiple columns
    together using a delimiter
    e.g. ["a", "b", "c"]
    => concat("a", " ", "b", " ", "c")
    """

    surrounded = [f'coalesce({name}, "")' for name in cols]
    with_spaces = f', "{delimiter}", '.join(surrounded)

    return f'md5(concat({with_spaces}))'


def blank_strings_to_nulls(df, columns):
    """
    turn blank strings into columns
    """

    if type(columns) == str:
        columns = [columns]

    for c in columns:
        df = df.withColumn(c, f.when(f.trim(f.col(c)) ==
                                 '', None).otherwise(f.col(c)))

    return df
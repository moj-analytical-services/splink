import re
import copy

import logging

log = logging.getLogger(__name__)
from .formatlog import format_sql

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

import logging
import re

from .logging_utils import log_sql
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns

log = logging.getLogger(__name__)


def add_null_treatment_to_case_statement(case_statement: str):
    """Add null treatment to user provided case statement if not already exists

    Args:
        case_statement (str): [description]

    Returns:
        str: case statement with null treatment added
    """

    sl = case_statement.lower()

    if "then -1" not in sl:
        variable_name = re.search(r"when ([\w_]{1,100})_l", case_statement)[1]
        find = r"(case)(\s+)(when)"
        replace = r"\1 \nwhen {col_name}_l is null or {col_name}_r is null then -1\n\3"
        new_case_statement = re.sub(find, replace, case_statement)
        new_case_statement = new_case_statement.format(col_name=variable_name)

        return new_case_statement
    else:
        return case_statement


def sql_gen_gammas_case_statement_2_levels(col_name, i):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{i}"""


def sql_gen_gammas_case_statement_3_levels(col_name, i):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= 0.3
    then 1
    else 0 end as gamma_{i}"""


def complete_settings_dict(gamma_settings_dict: dict):
    """Auto-populate any missing settings from the settings dictionary

    Args:
        gamma_settings_dict (dict): The settings dictionary

    Returns:
        dict: A gamma settings dictionary
    """

    case_lookup = {
        2: sql_gen_gammas_case_statement_2_levels,
        3: sql_gen_gammas_case_statement_3_levels,
    }

    gamma_counter = 0
    for col_name, col_value in gamma_settings_dict.items():

        col_value["gamma_index"] = gamma_counter

        if "col_name" not in col_value:
            col_value["col_name"] = col_name

        if "levels" not in col_value:
            col_value["levels"] = 2

        if "case_expression" not in col_value:
            col_value["case_expression"] = case_lookup[col_value["levels"]](
                col_name, gamma_counter
            )
        else:
            old_case_stmt = col_value["case_expression"]
            new_case_stmt = add_null_treatment_to_case_statement(old_case_stmt)
            col_value["case_expression"] = new_case_stmt

        gamma_counter += 1

    return gamma_settings_dict


def sql_gen_add_gammas(
    gamma_settings_dict: dict,
    include_orig_cols: bool = False,
    unique_id_col: str = "unique_id",
    table_name: str = "df_comparison",
):

    gamma_case_expressions = []
    for key in gamma_settings_dict:
        value = gamma_settings_dict[key]
        gamma_case_expressions.append(value["case_expression"])

    gammas_select_expr = ",\n".join(gamma_case_expressions)

    if include_orig_cols:
        orig_cols = gamma_settings_dict.keys()

        l = [f"{c}_l" for c in orig_cols]
        r = [f"{c}_r" for c in orig_cols]
        both = zip(l, r)
        flat_list = [item for sublist in both for item in sublist]
        orig_columns_select_expr = ", ".join(flat_list) + ", "
    else:
        orig_columns_select_expr = ""

    sql = f"""
    select {unique_id_col}_l, {unique_id_col}_r, {orig_columns_select_expr}{gammas_select_expr}
    from {table_name}
    """

    return sql


def add_gammas(df_comparison, gamma_settings_dict, spark=None, include_orig_cols=False):
    """

    """
    gamma_settings_dict = complete_settings_dict(gamma_settings_dict)

    sql = sql_gen_add_gammas(gamma_settings_dict)

    log_sql(sql, log)
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas

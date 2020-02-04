import logging
import re

from .logging_utils import log_sql
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns
from .case_statements import _add_null_treatment_to_case_statement, sql_gen_case_smnt_strict_equality_2, sql_gen_case_stmt_levenshtein_3

log = logging.getLogger(__name__)




def complete_settings_dict(gamma_settings_dict: dict):
    """Auto-populate any missing settings from the settings dictionary

    Args:
        gamma_settings_dict (dict): The settings dictionary

    Returns:
        dict: A gamma settings dictionary
    """

    case_lookup = {
        2: sql_gen_case_smnt_strict_equality_2,
        3: sql_gen_case_stmt_levenshtein_3
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
            new_case_stmt = _add_null_treatment_to_case_statement(old_case_stmt)
            col_value["case_expression"] = new_case_stmt

        gamma_counter += 1

    return gamma_settings_dict


def sql_gen_add_gammas(
    gamma_settings_dict: dict,
    include_orig_cols: bool = False,
    unique_id_col: str = "unique_id",
    table_name: str = "df_comparison",
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        gamma_settings_dict (dict): Gamma settings dict
        include_orig_cols (bool, optional): Whether to include original strings in output df. Defaults to False.
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".
        table_name (str, optional): Name of the comparison df. Defaults to "df_comparison".

    Returns:
        str: A SQL string
    """

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


def add_gammas(
    df_comparison,
    gamma_settings_dict,
    spark=None,
    include_orig_cols=False,
    unique_id_col: str = "unique_id",
):
    """[summary]

    Args:
        df_comparison (spark dataframe): A Spark dataframe containing record comparisons
        gamma_settings_dict (dict): The gamma settings dict
        spark (Spark session): The Spark session.
        include_orig_cols (bool, optional): Whether to include original string comparison columns or just leave gammas. Defaults to False.
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".

    Returns:
        Spark dataframe: A dataframe containing new columns representing the gammas of the model
    """

    gamma_settings_dict = complete_settings_dict(gamma_settings_dict)

    sql = sql_gen_add_gammas(
        gamma_settings_dict,
        include_orig_cols=include_orig_cols,
        unique_id_col=unique_id_col,
    )

    log_sql(sql, log)
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas

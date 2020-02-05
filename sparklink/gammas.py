import logging
import re
import warnings

from .logging_utils import log_sql
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns
from .case_statements import (
    _add_null_treatment_to_case_statement,
    _check_jaro_registered,
    _check_no_obvious_problem_with_case_statement,
    _add_as_gamma_to_case_statement,
    sql_gen_case_smnt_strict_equality_2,
    sql_gen_case_stmt_levenshtein_3,
    sql_gen_case_stmt_levenshtein_4,
    sql_gen_gammas_case_stmt_jaro_2,
    sql_gen_gammas_case_stmt_jaro_3,
    sql_gen_gammas_case_stmt_jaro_4,
    sql_gen_case_stmt_numeric_2,
    sql_gen_case_stmt_numeric_perc_3,
    sql_gen_case_stmt_numeric_perc_4,
)

log = logging.getLogger(__name__)


def _normalise_prob_list(prob_array: list):
    sum_list = sum(prob_array)
    return [i/sum_list for i in prob_array]


def _get_default_case_statements_functions(spark):
    default_case_statements = {
            "numeric": {
                2: sql_gen_case_stmt_numeric_2,
                3: sql_gen_case_stmt_numeric_perc_3,
                4: sql_gen_case_stmt_numeric_perc_3,
            },
            "string": {}
        }

    jaro_exists = _check_jaro_registered(spark)


    if jaro_exists :
         default_case_statements["string"][2] = sql_gen_gammas_case_stmt_jaro_2
         default_case_statements["string"][3] = sql_gen_gammas_case_stmt_jaro_3
         default_case_statements["string"][4] = sql_gen_gammas_case_stmt_jaro_4

    else:
        default_case_statements["string"][2] = sql_gen_case_smnt_strict_equality_2
        default_case_statements["string"][3] = sql_gen_case_stmt_levenshtein_3
        default_case_statements["string"][4] = sql_gen_case_stmt_levenshtein_4

    return default_case_statements

def _get_default_case_statement_fn(default_statements, data_type, levels):
    if data_type not in ["string", "numeric"]:
        raise ValueError(f"No default case statement available for data type {data_type}, "
                          "please specify a custom case_expression")
    if levels > 4:
        raise ValueError(f"No default case statement available when levels > 4, "
                          "please specify a custom 'case_expression' within your settings dictionary")
    return default_statements[data_type][levels]

def _get_probabilities(m_or_u, levels):

    if levels > 4:
        raise ValueError(f"No default m and u probabilities available when levels > 4, "
                          "please specify custom values for 'm_probabilities' and æu_probabilities' "
                          "within your settings dictionary")

    # Note all m and u probabilities are automatically normalised to sum to 1
    default_m_u_probabilities = {
        "m": {
            2: [1,9],
            3: [1,2,7],
            4: [1,1,1,7]
        },
        "u": {
            2: [9,1],
            3: [7,2,1],
            4: [7,1,1,1]
        }
    }

    probabilities = default_m_u_probabilities[m_or_u][levels]
    return _normalise_prob_list(probabilities)


def complete_settings_dict(gamma_settings_dict: dict, spark=None):
    """Auto-populate any missing settings from the settings dictionary

    Args:
        gamma_settings_dict (dict): The settings dictionary
        spark: The SparkSession

    Returns:
        dict: A gamma settings dictionary
    """

    default_case_statements = _get_default_case_statements_functions(spark)

    gamma_counter = 0
    for col_name, col_value in gamma_settings_dict.items():

        col_value["gamma_index"] = gamma_counter

        if "col_name" not in col_value:
            col_value["col_name"] = col_name

        if "levels" not in col_value:
            col_value["levels"] = 2

        if "data_type" not in col_value:
            col_value["data_type"] = "string"

        if "case_expression" not in col_value:
            data_type = col_value["data_type"]
            levels = col_value["levels"]
            case_fn = _get_default_case_statement_fn(default_case_statements, data_type, levels)
            col_value["case_expression"] = case_fn(col_name, gamma_counter)
        else:
            _check_no_obvious_problem_with_case_statement(col_value["case_expression"])
            old_case_stmt = col_value["case_expression"]
            new_case_stmt = _add_null_treatment_to_case_statement(old_case_stmt)
            new_case_stmt = _add_as_gamma_to_case_statement(new_case_stmt, gamma_counter)
            col_value["case_expression"] = new_case_stmt

        if "m_probabilities" not in col_value:
            levels = col_value["levels"]
            probs = _get_probabilities("m", levels)
            col_value["m_probabilities"] = probs
        else:
            levels = col_value["levels"]
            probs = col_value["m_probabilities"]
            if len(probs) != levels:
                raise ValueError("Number of m probabilities provided is not equal to number of levels specified")

        if "u_probabilities" not in col_value:
            levels = col_value["levels"]
            probs = _get_probabilities("u", levels)
            col_value["u_probabilities"] = probs
        else:
            levels = col_value["levels"]
            probs = col_value["u_probabilities"]
            if len(probs) != levels:
                raise ValueError("Number of m probabilities provided is not equal to number of levels specified")

        col_value["m_probabilities"] = _normalise_prob_list(col_value["m_probabilities"])
        col_value["u_probabilities"] = _normalise_prob_list(col_value["u_probabilities"])


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

    gamma_settings_dict = complete_settings_dict(gamma_settings_dict, spark)

    sql = sql_gen_add_gammas(
        gamma_settings_dict,
        include_orig_cols=include_orig_cols,
        unique_id_col=unique_id_col,
    )

    log_sql(sql, log)
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas

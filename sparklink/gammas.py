import logging
import re
import warnings
import jsonschema

from .logging_utils import log_sql
from .validate import validate_settings, _get_default_value
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


def complete_settings_dict(settings_dict: dict, spark=None):
    """Auto-populate any missing settings from the settings dictionary

    Args:
        settings_dict (dict): The settings dictionary
        spark: The SparkSession

    Returns:
        dict: A gamma settings dictionary
    """
    validate_settings(settings_dict)
    default_case_statements = _get_default_case_statements_functions(spark)

    # Complete non-column settings
    non_col_keys = ["em_convergence",
                    "unique_id_column_name",
                    "other_columns_to_retain"]
    for key in non_col_keys:
        if key not in settings_dict:
            settings_dict[key] =  _get_default_value(key, is_column_setting=False)

    if "proportion_of_matches" not in settings_dict:
        settings_dict["proportion_of_matches"] = _get_default_value("proportion_of_matches", is_column_setting=False)

    gamma_counter = 0
    for gamma_counter, column_settings in enumerate(settings_dict["comparison_columns"]):

        column_settings["gamma_index"] = gamma_counter
        col_name = column_settings["col_name"]

        # Populate non-existing keys from defaults
        for key in ["num_levels", "data_type"]:
            if key not in column_settings:
                default = _get_default_value(key, is_column_setting=True)
                column_settings[key] = default

        if "case_expression" not in column_settings:
            data_type = column_settings["data_type"]
            levels = column_settings["num_levels"]
            case_fn = _get_default_case_statement_fn(default_case_statements, data_type, levels)
            column_settings["case_expression"] = case_fn(col_name, gamma_counter)
        else:
            _check_no_obvious_problem_with_case_statement(column_settings["case_expression"])
            old_case_stmt = column_settings["case_expression"]
            new_case_stmt = _add_null_treatment_to_case_statement(old_case_stmt)
            new_case_stmt = _add_as_gamma_to_case_statement(new_case_stmt, gamma_counter)
            column_settings["case_expression"] = new_case_stmt

        if "m_probabilities" not in column_settings:
            levels = column_settings["num_levels"]
            probs = _get_probabilities("m", levels)
            column_settings["m_probabilities"] = probs
        else:
            levels = column_settings["num_levels"]
            probs = column_settings["m_probabilities"]

            if len(probs) != levels:
                raise ValueError("Number of m probabilities provided is not equal to number of levels specified")

        if "u_probabilities" not in column_settings:
            levels = column_settings["num_levels"]
            probs = _get_probabilities("u", levels)
            column_settings["u_probabilities"] = probs
        else:
            levels = column_settings["num_levels"]
            probs = column_settings["u_probabilities"]
            if len(probs) != levels:
                raise ValueError("Number of m probabilities provided is not equal to number of levels specified")

        column_settings["m_probabilities"] = _normalise_prob_list(column_settings["m_probabilities"])
        column_settings["u_probabilities"] = _normalise_prob_list(column_settings["u_probabilities"])

        gamma_counter += 1

    return settings_dict


def sql_gen_add_gammas(
    settings_dict: dict,
    include_orig_cols: bool = False,
    unique_id_col: str = "unique_id",
    table_name: str = "df_comparison",
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings_dict (dict): Gamma settings dict
        include_orig_cols (bool, optional): Whether to include original strings in output df. Defaults to False.
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".
        table_name (str, optional): Name of the comparison df. Defaults to "df_comparison".

    Returns:
        str: A SQL string
    """

    gamma_case_expressions = []
    for value in settings_dict["comparison_columns"]:
        gamma_case_expressions.append(value["case_expression"])

    gammas_select_expr = ",\n".join(gamma_case_expressions)

    if include_orig_cols:
        orig_cols = settings_dict["comparison_columns"]
        orig_cols = [c["col_name"] for c in orig_cols]

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
    settings_dict,
    spark=None,
    include_orig_cols=False,
    unique_id_col: str = "unique_id",
):
    """[summary]

    Args:
        df_comparison (spark dataframe): A Spark dataframe containing record comparisons
        settings_dict (dict): The gamma settings dict
        spark (Spark session): The Spark session.
        include_orig_cols (bool, optional): Whether to include original string comparison columns or just leave gammas. Defaults to False.
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".

    Returns:
        Spark dataframe: A dataframe containing new columns representing the gammas of the model
    """




    settings_dict = complete_settings_dict(settings_dict, spark)

    sql = sql_gen_add_gammas(
        settings_dict,
        include_orig_cols=include_orig_cols,
        unique_id_col=unique_id_col,
    )

    log_sql(sql, log)
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas

import logging

from .logging_utils import format_sql
from .sql import comparison_columns_select_expr

log = logging.getLogger(__name__)


# Generate gammas dataset
def gammas_case_statement_2_levels(col_name, i):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{i}"""


def gammas_case_statement_3_levels(col_name, i):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= 0.3
    then 1
    else 0 end as gamma_{i}"""


def complete_settings_dict(gamma_settings_dict):
    """
    Where the user has omitted details from the settings dict,
    populate them with the defaults
    """

    case_lookup = {
        2: gammas_case_statement_2_levels,
        3: gammas_case_statement_3_levels
    }

    gamma_counter = 0
    for col_name, col_value in gamma_settings_dict.items():

        col_value["gamma_index"] = gamma_counter

        if "col_name" not in col_value:
            col_value["col_name"] = col_name

        if "levels" not in col_value:
            col_value["levels"] = 2

        if "case_expression" not in col_value:
            col_value["case_expression"] = case_lookup[col_value["levels"]](col_name, gamma_counter)

        gamma_counter += 1

    return gamma_settings_dict


def add_gammas(df, gamma_settings_dict, spark, include_orig_cols=False):
    """

    """
    gamma_settings_dict = complete_settings_dict(gamma_settings_dict)

    gamma_case_expressions = []
    for key, value in gamma_settings_dict.items():
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


    df.registerTempTable("df")
    sql = f"""
    select {orig_columns_select_expr} unique_id_l, unique_id_r, {gammas_select_expr}
    from df
    """

    df = spark.sql(sql)
    log.debug(format_sql(sql))
    return df

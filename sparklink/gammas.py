import logging

from formatlog import format_sql
from sql import comparison_columns_select_expr

log = logging.getLogger(__name__)


# Generate gammas dataset
def gammas_case_statement_2_levels(col_name, i):
    return f"""case
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{i}"""


def gammas_case_statement_3_levels(col_name, i):
    return f"""case
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r) <= 5 then 1
    else 0 end as gamma_{i}"""




def add_gammas(df, setting_dict, spark, override_case_statements = None):
    """

    """
    # TODO: Implement override_case_statements customisability

    case_statement_functions = {
        "2_levels": gammas_case_statement_2_levels
        "3_levels": gammas_case_statement_3_levels
    }

    gamma_select_expressions = []
    for i, col_name in enumerate(binary_comparison_cols):
        gamma_select_expressions.append(gammas_case_statement(col_name, i))

    gammas_select_expr = ",\n".join(gamma_select_expressions)

    df.registerTempTable("df")
    sql = f"""
    select unique_id_l, unique_id_r, {gammas_select_expr}
    from df
    """

    df = spark.sql(sql)
    log.debug(format_sql(sql))
    return df

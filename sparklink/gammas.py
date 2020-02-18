import logging
import re
import warnings
import jsonschema

from .logging_utils import log_sql
from .validate import validate_settings, _get_default_value
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns
from .settings import complete_settings_dict

log = logging.getLogger(__name__)


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

    # gamma_case_expressions = []
    # for value in settings_dict["comparison_columns"]:
        # gamma_case_expressions.append(value["case_expression"])

    # gammas_select_expr = ",\n".join(gamma_case_expressions)

    select_cols = []
    for col in settings_dict["comparison_columns"]:
        if include_orig_cols:
            select_cols.append(col["col_name"] + "_l")
            select_cols.append(col["col_name"] + "_r")
        select_cols.append(col["case_expression"])
        # select_cols.append("gamma_" + col["col_name"])

    select_cols_expr = ", ".join(select_cols)

    sql = f"""
    select {unique_id_col}_l, {unique_id_col}_r, {select_cols_expr}
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

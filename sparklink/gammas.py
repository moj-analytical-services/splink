import logging
import re
import warnings
import jsonschema
from collections import OrderedDict

from .logging_utils import log_sql
from .validate import validate_settings, _get_default_value
from .sql import comparison_columns_select_expr, sql_gen_comparison_columns
from .settings import complete_settings_dict

log = logging.getLogger(__name__)


def _add_left_right(columns_to_retain, name):
    columns_to_retain[name + "_l"] = name + "_l"
    columns_to_retain[name + "_r"] = name + "_r"
    return columns_to_retain

def _get_select_expression_gammas(settings: dict):
    """Get a select expression which picks which columns to keep in df_gammas

    Args:
        settings (dict): A `sparklink` settings dictionary

    Returns:
        str: A select expression
    """

    # Use ordered dict as an ordered set - i.e. to make sure we don't have duplicate cols to retain

    cols_to_retain = OrderedDict()
    cols_to_retain = _add_left_right(cols_to_retain, settings["unique_id_column_name"])

    for col in settings["comparison_columns"]:
        col_name = col["col_name"]
        if settings["retain_matching_columns"]:
            cols_to_retain = _add_left_right(cols_to_retain, col_name)
        if col["term_frequency_adjustments"]:
            cols_to_retain = _add_left_right(cols_to_retain, col_name)
        cols_to_retain["gamma_" + col_name] = col["case_expression"]

    for c in settings["additional_columns_to_retain"]:
        cols_to_retain[c] = c

    return ", ".join(cols_to_retain.values())


def sql_gen_add_gammas(
    settings: dict,
    include_orig_cols: bool = False,
    unique_id_col: str = "unique_id",
    table_name: str = "df_comparison",
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings (dict): Gamma settings dict
        include_orig_cols (bool, optional): Whether to include original strings in output df. Defaults to False.
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".
        table_name (str, optional): Name of the comparison df. Defaults to "df_comparison".

    Returns:
        str: A SQL string
    """


    select_cols_expr = _get_select_expression_gammas(settings)

    sql = f"""
    select {select_cols_expr}
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

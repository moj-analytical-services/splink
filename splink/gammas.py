from collections import OrderedDict
import logging
import re
import warnings

try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

from .check_types import check_types
from .logging_utils import _format_sql
from .settings import complete_settings_dict
from .validate import validate_settings, _get_default_value

logger = logging.getLogger(__name__)

def _add_left_right(columns_to_retain, name):
    columns_to_retain[name + "_l"] = name + "_l"
    columns_to_retain[name + "_r"] = name + "_r"
    return columns_to_retain

def _get_select_expression_gammas(settings: dict):
    """Get a select expression which picks which columns to keep in df_gammas

    Args:
        settings (dict): A `splink` settings dictionary

    Returns:
        str: A select expression
    """

    # Use ordered dict as an ordered set - i.e. to make sure we don't have duplicate cols to retain

    cols_to_retain = OrderedDict()
    cols_to_retain = _add_left_right(cols_to_retain, settings["unique_id_column_name"])

    for col in settings["comparison_columns"]:
        if "col_name" in col:
            col_name = col["col_name"]
            if settings["retain_matching_columns"]:
                cols_to_retain = _add_left_right(cols_to_retain, col_name)
            if col["term_frequency_adjustments"]:
                cols_to_retain = _add_left_right(cols_to_retain, col_name)
            cols_to_retain["gamma_" + col_name] = col["case_expression"]
        if "custom_name" in col:
            custon_name = col["custom_name"]
            if settings["retain_matching_columns"]:
                for c2 in col["custom_columns_used"]:
                    cols_to_retain = _add_left_right(cols_to_retain, c2)
            cols_to_retain["gamma_" + custon_name] = col["case_expression"]


    if settings["link_type"] == 'link_and_dedupe':
        cols_to_retain = _add_left_right(cols_to_retain, "_source_table")

    for c in settings["additional_columns_to_retain"]:
        cols_to_retain = _add_left_right(cols_to_retain, c)

    return ", ".join(cols_to_retain.values())


def _sql_gen_add_gammas(
    settings: dict,
    unique_id_col: str = "unique_id",
    table_name: str = "df_comparison",
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings (dict): `splink` settings dict
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


@check_types
def add_gammas(
    df_comparison: DataFrame,
    settings_dict: dict,
    spark:SparkSession,
    unique_id_col: str = "unique_id",
):
    """ Compute the comparison vectors and add them to the dataframe.  See
    https://imai.fas.harvard.edu/research/files/linkage.pdf for more details of what is meant by comparison vectors

    Args:
        df_comparison (spark dataframe): A Spark dataframe containing record comparisons, with records compared using the convention col_name_l, col_name_r
        settings_dict (dict): The `splink` settings dictionary
        spark (Spark session): The Spark session object
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".

    Returns:
        Spark dataframe: A dataframe containing new columns representing the gammas of the model
    """


    settings_dict = complete_settings_dict(settings_dict, spark)

    sql = _sql_gen_add_gammas(
        settings_dict,
        unique_id_col=unique_id_col,
    )

    logger.debug(_format_sql(sql))
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas

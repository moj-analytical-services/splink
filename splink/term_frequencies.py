# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107

import logging

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from .logging_utils import _format_sql
from .model import Model

logger = logging.getLogger(__name__)


def sql_gen_term_frequencies(column_name, table_name="df"):

    sql = f"""
    select
    {column_name}, count(*) / (select
        count({column_name}) as total from {table_name}) as tf_{column_name}
    from {table_name}
    where {column_name} is not null
    group by {column_name}
    """

    return sql


def _sql_gen_add_term_frequencies(model: Model, table_name: str = "df"):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings (dict): `splink` settings dict
        table_name (str, optional): Name of the source df. Defaults to "df".

    Returns:
        str: A SQL string
    """

    cc_dict = model.current_settings_obj.comparison_column_dict

    cols = [name for name, cc in cc_dict.items() if cc.term_frequency_adjustments]
    # cols = [cc.name for cc in settings["comparison_columns"] if cc["term_frequency_adjustments"]]
    tf_tables = ", ".join(
        [f"tf_{col} as ({sql_gen_term_frequencies(col, table_name)})" for col in cols]
    )

    tf_cols = ", ".join(f"tf_{col}.tf_{col}" for col in cols)

    joins = "".join(
        [
            f"""
    left join tf_{col}
    on {table_name}.{col} = tf_{col}.{col}
    """
            for col in cols
        ]
    )

    sql = f"""
    with {tf_tables}
    select {table_name}.*, {tf_cols}
    from {table_name}
    {joins}
    """

    return sql


def add_term_frequencies(df: DataFrame, model: Model, spark: SparkSession):
    """Compute the term frequencies of the required columns and add to the dataframe.
    Args:
        df (spark dataframe): A Spark dataframe containing source records for linking
        settings_dict (dict): The `splink` settings dictionary
        spark (Spark session): The Spark session object

    Returns:
        Spark dataframe: A dataframe containing new columns representing the term frequencies
        of the corresponding values
    """

    if model.current_settings_obj.any_cols_have_tf_adjustments:
        sql = _sql_gen_add_term_frequencies(model, "df")

        logger.debug(_format_sql(sql))
        df.createOrReplaceTempView("df")
        df_with_tf = spark.sql(sql)

        return df_with_tf
    else:
        return df

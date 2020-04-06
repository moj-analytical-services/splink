import logging

# For type hints. Try except to ensure the sql_gen functions even if spark doesn't exist.
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

logger = logging.getLogger(__name__)
from .logging_utils import _format_sql
from .params import Params


def _sql_gen_new_lambda(table_name = "df_intermediate"):

    sql = f"""
    select cast(sum(expected_num_matches)/sum(num_rows) as float) as new_lambda
    from {table_name}
    """

    return sql


def _get_new_lambda(df_intermediate, spark):
    """
    Calculate lambda, as expected proportion of matches
    given current parameter estimates.

    This can then be used in future iterations.
    """
    df_intermediate.createOrReplaceTempView("df_intermediate")
    sql = _sql_gen_new_lambda(table_name = "df_intermediate")

    new_lambda = spark.sql(sql).collect()[0][0]
    logger.debug(_format_sql(sql))
    return new_lambda


def _sql_gen_intermediate_pi_aggregate(params, table_name="df_e"):
    """
    This intermediate step is calculated for efficiency purposes.

    In the maximisation step, to compute the new
    pi probability distributions, we need to perform a variety of calculations that can all be derived
    from this intermediate table.

    Without this intermediate table, we'd be repeating these calculations multiple times.
    """

    gamma_cols_expr = ", ".join(params._gamma_cols)

    sql = f"""
    select {gamma_cols_expr}, sum(match_probability) as expected_num_matches, sum(1- match_probability) as expected_num_non_matches, count(*) as num_rows
    from {table_name}
    group by {gamma_cols_expr}
    """
    return sql


def _sql_gen_pi_df(params, table_name="df_intermediate"):

    sqls = []
    for gamma_str in params._gamma_cols:
        sql = f"""
        select {gamma_str} as gamma_value,
        cast(sum(expected_num_matches)/(select sum(expected_num_matches) from {table_name} where {gamma_str} != -1) as float) as new_probability_match,
        cast(sum(expected_num_non_matches)/(select sum(expected_num_non_matches) from {table_name} where {gamma_str} != -1) as float) as new_probability_non_match,
        '{gamma_str}' as gamma_col
        from {table_name}
        group by {gamma_str}
        """
        sqls.append(sql)

    sql = "\nunion all\n".join(sqls)

    return sql



def _get_new_pi_df(df_intermediate, spark, params):
    """
    Calculate and collect a dataframe that contains all the new values of pi
    """
    df_intermediate.createOrReplaceTempView("df_intermediate")
    sql = _sql_gen_pi_df(params)
    levels = spark.sql(sql).collect()
    logger.debug(_format_sql(sql))
    return [l.asDict() for l in levels]



def run_maximisation_step(df_e: DataFrame, params:Params, spark:SparkSession):
    """Compute new parameters and save them in the params object

    Note that the params object will be updated in-place by this function

    Args:
        df_e (DataFrame): the result of the expectation step
        params (Params): splink Params object
        spark (SparkSession): The spark session
    """

    sql = _sql_gen_intermediate_pi_aggregate(params)

    df_e.createOrReplaceTempView("df_e")
    df_intermediate = spark.sql(sql)
    logger.debug(_format_sql(sql))
    df_intermediate.createOrReplaceTempView("df_intermediate")
    df_intermediate.persist()

    new_lambda = _get_new_lambda(df_intermediate,  spark)
    pi_df_collected = _get_new_pi_df(df_intermediate, spark, params)

    params._update_params(new_lambda, pi_df_collected)
    df_intermediate.unpersist()

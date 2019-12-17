import logging

log = logging.getLogger(__name__)
from .logging_utils import format_sql

def sql_gen_new_lambda(table_name = "df_e"):

    sql = f"""
    select cast(sum(match_probability)/count(*) as float) as new_lambda
    from {table_name}
    """

    return sql


def get_new_lambda(df_e, spark):
    """
    Calculate lambda, as expected proportion of matches
    given current parameter estimates.

    This can then be used in future iterations.
    """

    sql = sql_gen_new_lambda(table_name = "df_e")
    df_e.registerTempTable("df_e")

    new_lambda = spark.sql(sql).collect()[0][0]
    log.debug(format_sql(sql))
    return new_lambda


def sql_gen_intermediate_pi_aggregate(params, table_name="df_e"):
    """
    This intermediate step is calculated for efficiency purposes.

    In the maximisation step, to compute the new
    pi probability distributions, we need to perform a variety of calculations that can all be derived
    from this intermediate table.

    Without this intermediate table, we'd be repeating these calculations multiple times.
    """

    gamma_cols_expr = ", ".join(params.gamma_cols)

    sql = f"""
    select {gamma_cols_expr}, sum(match_probability) as expected_num_matches, sum(1- match_probability) as expected_num_non_matches, count(*) as num_rows
    from {table_name}
    group by {gamma_cols_expr}
    """
    return sql


def sql_gen_pi_df(params, table_name="df_intermediate"):

    sqls = []

    for gamma_str in params.gamma_cols:
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



def get_new_pi_df(df_e, spark, params):
    """
    Calculate and collect a dataframe that contains all the new values of pi
    """

    sql = sql_gen_pi_df(params)
    levels = spark.sql(sql).collect()
    log.debug(format_sql(sql))
    return [l.asDict() for l in levels]



def run_maximisation_step(df_e, spark, params):
    """
    Compute new parameters and save them in the params dict
    """


    sql = sql_gen_intermediate_pi_aggregate(params)

    df_e.registerTempTable("df_e")
    df_intermediate = spark.sql(sql)
    log.debug(format_sql(sql))
    df_intermediate.registerTempTable("df_intermediate")
    df_intermediate.persist()

    new_lambda = get_new_lambda(df_e,  spark)
    pi_df_collected = get_new_pi_df(df_e, spark, params)

    params.update_params(new_lambda, pi_df_collected)
    df_intermediate.unpersist()

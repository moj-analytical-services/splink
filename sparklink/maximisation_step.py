import logging

log = logging.getLogger(__name__)
from .formatlog import format_sql

def get_new_lambda(df_e, spark):
    """
    Calculate lambda, as expected proportion of matches
    given current parameter estimates.

    This can then be used in future iterations.
    """

    df_e.registerTempTable("df_e")
    sql = """
    select cast(sum(match_probability)/count(*) as float) as new_lambda
    from df_e
    """
    new_lambda = spark.sql(sql).collect()[0][0]
    log.debug(format_sql(sql))
    return new_lambda


def sql_gen_intermediate_pi_aggregate(params):
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
    from df_e
    group by {gamma_cols_expr}

    """
    return sql


def get_new_pi_df(df_e, spark, params):
    """
    Calculate and collect a dataframe that contains all the new values of pi
    """

    sqls = []

    for gamma_str in params.gamma_cols:
        sql = f"""
        select {gamma_str} as gamma_value,
        cast(sum(expected_num_matches)/(select sum(expected_num_matches) from df_intermediate where {gamma_str} != -1) as float) as new_probability_match,
        cast(sum(expected_num_non_matches)/(select sum(expected_num_non_matches) from df_intermediate where {gamma_str} != -1) as float) as new_probability_non_match,
        '{gamma_str}' as gamma_col
        from df_intermediate
        group by {gamma_str}
        """
        sqls.append(sql)

    sql = "\nunion all\n".join(sqls)

    levels = spark.sql(sql).collect()
    log.debug(format_sql(sql))
    return levels



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

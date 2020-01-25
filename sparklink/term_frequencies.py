# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/sparklink/issues/17

import logging

from .logging_utils import log_sql, format_sql

log = logging.getLogger(__name__)

def sql_gen_bayes_string(probs):
    """Convenience function for computing an updated probability using bayes' rule

    e.g. if probs = ['p1', 'p2', 0.3]

    return the sql expression 'p1*p2*0.3/(p1*p2*0.3 + (1-p1)*(1-p2)*(1-0.3))'

    Args:
        probs: Array of column names or constant values

    Returns:
        string: a sql expression
    """

    # Needed in case e.g. float constant value passed
    probs = [f"cast({p} as double)" for p in probs]

    inverse_probs = [f"(cast(1 - {p} as double))" for p in probs]

    probs_multiplied = " * ".join(probs)
    inverse_probs_multiplied = " * ".join(inverse_probs)

    return f"""
    {probs_multiplied}/
    (  {probs_multiplied} + {inverse_probs_multiplied} )
    """


def sql_gen_generate_adjusted_lambda(column_name, params, table_name='df_e'):

    sql = f"""
    with temp_adj as
    (
    select {column_name}_l, {column_name}_r, sum(match_probability)/count(match_probability) as adj_lambda
    from {table_name}
    where {column_name}_l = {column_name}_r
    group by {column_name}_l, {column_name}_r
    )

    select {column_name}_l, {column_name}_r, {sql_gen_bayes_string(["adj_lambda", 1-params.params["λ"]])}
    as {column_name}_adj_nulls
    from temp_adj
    """

    return sql

def sql_gen_add_adjumentments_to_df_e(term_freq_column_list):

    coalesce_template = "coalesce({c}_adj_nulls, 0.5) as {c}_adj"
    coalesces =  [coalesce_template.format(c=c) for c in term_freq_column_list]
    coalesces = ",\n ".join(coalesces)

    left_join_template = """
     left join
    {c}_lookup
    on {c}_lookup.{c}_l = e.{c}_l
    and {c}_lookup.{c}_r = e.{c}_r
    """

    left_joins = [left_join_template.format(c=c) for c in term_freq_column_list]
    left_joins = "\n ".join(left_joins)



    broadcast_hints = [f"BROADCAST({c}_lookup)" for c in term_freq_column_list]
    broadcast_hint = " ".join(broadcast_hints)
    broadcast_hint = f" /*+  {broadcast_hint} */ "


    sql = f"""
    select {broadcast_hint} e.*, {coalesces}
    from df_e as e

    {left_joins}
    """

    return sql


def sql_gen_compute_final_group_membership_prob_from_adjustments(term_freq_column_list, table_name="df_e_adj"):

    term_freq_column_list = [c + "_adj" for c in term_freq_column_list]
    term_freq_column_list.insert(0, "match_probability")
    sql = f"""
    select *, {sql_gen_bayes_string(term_freq_column_list)} as tf_adjusted_match_prob
    from {table_name}
    """

    return sql


def make_adjustment_for_term_frequencies(df_e, params, term_freq_column_list, retain_adjustment_columns=False, spark=None, logger=log):

    df_e.createOrReplaceTempView("df_e")

    # Generate a lookup table for each column with 'term specific' lambdas.
    for c in term_freq_column_list:
        sql = sql_gen_generate_adjusted_lambda(c, params)
        log_sql(sql, logger)
        lookup = spark.sql(sql)
        lookup.createOrReplaceTempView(f"{c}_lookup")

    # Merge these lookup tables into main table
    sql  = sql_gen_add_adjumentments_to_df_e(term_freq_column_list)
    log_sql(sql, logger)
    df_e_adj = spark.sql(sql)
    df_e_adj.createOrReplaceTempView("df_e_adj")

    sql = sql_gen_compute_final_group_membership_prob_from_adjustments(term_freq_column_list)
    log_sql(sql, logger)
    df = spark.sql(sql)
    if not retain_adjustment_columns:
        for c in term_freq_column_list:
            df = df.drop(c+ "_adj")

    return df




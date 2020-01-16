"""
In the expectation step we calculate the membership probabilities
i.e. for each comparison, what is the probability that it's a member
of group match = 0 and group match = 1
"""

import logging

log = logging.getLogger(__name__)
from .logging_utils import log_sql, log_other

def run_expectation_step(df_with_gamma, spark, params, compute_ll=False, logger=log):
    """[summary]

    Args:
        df_with_gamma ([type]): [description]
        spark ([type]): [description]
        params ([type]): [description]
        compute_ll (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    sql = sql_gen_gamma_prob_columns(params)

    df_with_gamma.createOrReplaceTempView("df_with_gamma")
    log_sql(sql, logger)
    df_with_gamma_probs = spark.sql(sql)
    df_with_gamma_probs.persist()


    # This is optional because is slows down execution
    if compute_ll:
        ll = get_overall_log_likelihood(df_with_gamma_probs, params, spark)
        message = f"Log likelihood for iteration {params.iteration-1}:  {ll}"
        log_other(message, logger, level='INFO')
        params.params["log_likelihood"] = ll

    sql = sql_gen_expected_match_prob(params)

    log_sql(sql, logger)
    df_with_gamma_probs.createOrReplaceTempView("df_with_gamma_probs")
    df_e = spark.sql(sql)

    df_e.createOrReplaceTempView("df_e")
    return df_e


def sql_gen_gamma_prob_columns(params, table_name="df_with_gamma"):
    """
    For each row, look up the probability of observing the gamma value given the record
    is a match and non_match respectively
    """

    case_statements = []
    for gamma_str in params.gamma_cols:
        for match in [0, 1]:
            case_statements.append(
                sql_gen_gamma_case_when(gamma_str, match, params))

    case_statements = ", \n\n".join(case_statements)

    sql = f"""
    -- We use case statements for these lookups rather than joins for performance and simplicity
    select *,
    {case_statements}
    from {table_name}
    """

    return sql


def sql_gen_expected_match_prob(params, table_name="df_with_gamma_probs"):
    gamma_cols = params.gamma_cols

    numerator = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    denom_part = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])

    λ = params.params['λ']
    match_prob_expression = f"({λ} * {numerator})/(( {λ} * {numerator}) + ({1 -λ} * {denom_part})) as match_probability"

    sql = f"""
    select *,
    {match_prob_expression}
    from {table_name}
    """

    return sql


def sql_gen_gamma_case_when(gamma_str, match, params):
    """
    Create the case statements that look up the correct probabilities in the
    params dict for each gamma
    """

    if match == 1:
        dist = "prob_dist_match"
    if match == 0:
        dist = "prob_dist_non_match"

    levels = params.params["π"][gamma_str][dist]

    case_statements = []
    case_statements.append(f"WHEN {gamma_str} = -1 THEN 1")
    for key, level in levels.items():
            case_stmt = f"when {gamma_str} = {level['value']} then {level['probability']}"
            case_statements.append(case_stmt)

    case_statements = "\n".join(case_statements)

    if match == 1:
        name_suffix = "_match"
    if match == 0:
        name_suffix = "_non_match"

    sql = f""" case \n{case_statements} \nend \nas prob_{gamma_str}{name_suffix}"""

    return sql.strip()


def calculate_log_likelihood_df(df_with_gamma_probs, params, spark, logger=log):
    """
    Compute likelihood of observing df_with_gamma given the parameters

    Likelihood is just ((1-lambda) * prob not match) * (lambda * prob match)
    """


    gamma_cols = params.gamma_cols

    λ = params.params['λ']

    match_prob = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    match_prob = f"({λ} * {match_prob})"
    non_match_prob = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])
    non_match_prob = f"({1-λ} * {non_match_prob})"
    log_likelihood = f"ln({match_prob} + {non_match_prob})"

    numerator = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    denom_part = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])
    match_prob_expression = f"({λ} * {numerator})/(( {λ} * {numerator}) + ({1 -λ} * {denom_part})) as match_probability"

    df_with_gamma_probs.createOrReplaceTempView("df_with_gamma_probs")
    sql = f"""
    select *,
    cast({log_likelihood} as float) as  log_likelihood,
    {match_prob_expression}

    from df_with_gamma_probs
    """
    log_sql(sql, logger)
    df = spark.sql(sql)

    return df


def get_overall_log_likelihood(df_with_gamma_probs, params, spark):

    df = calculate_log_likelihood_df(df_with_gamma_probs, params, spark)
    return df.groupby().sum("log_likelihood").collect()[0][0]

import logging

log = logging.getLogger(__name__)
from formatlog import format_sql

def run_expectation_step(df_with_gamma, spark, params):

    sql = sql_gen_gamma_prob_columns(df_with_gamma, params)

    df_with_gamma.registerTempTable("df_with_gamma")
    df_with_gamma_probs = spark.sql(sql)
    log.debug(format_sql(sql))

    sql = sql_gen_expected_match_prob(df_with_gamma_probs, params)
    df_with_gamma_probs.registerTempTable("df_with_gamma_probs")
    df_e = spark.sql(sql)
    log.debug(format_sql(sql))
    df_e.registerTempTable("df_e")
    return df_e


def sql_gen_gamma_prob_columns(df_with_gamma, params):
    """
    For each row, look up the probability of observing the gamma value given the record
    is a match and non_match respectively
    """

    gamma_cols_to_select = ", ".join(params.gamma_cols)

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
    from df_with_gamma
    """

    return sql


def sql_gen_expected_match_prob(df, params):
    gamma_cols = params.gamma_cols
    numerator = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    denom_part = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])

    λ = params.params['λ']
    match_prob_expression = f"({λ} * {numerator})/(( {λ} * {numerator}) + ({1 -λ} * {denom_part})) as match_probability"

    sql = f"""
    select *,
    {match_prob_expression}
    from df_with_gamma_probs
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

def calculate_likelihood(df_gammas, params, spark):
    """
    Compute likelihood of observing df_gammas given the parameters
    """
    pass
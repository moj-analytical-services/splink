import re
import copy

import logging

log = logging.getLogger(__name__)


def comparison_columns_select_expr(df):
    """
    Compare cols in df
    Example output from a input df with columns [first_name,  surname]
    l.first_name as first_name_l, r.first_name as first_name_r, l.surname as surname_l, r.surname as surname_r
    """

    l = [f"l.{c} as {c}_l" for c in df.columns]
    r = [f"r.{c} as {c}_r" for c in df.columns]
    both = zip(l, r)
    flat_list = [item for sublist in both for item in sublist]
    return ", ".join(flat_list)


def sql_gen_gamma_case_when(gamma_str, match, params):

    if match == 1:
        dist = "prob_dist_match"
    if match == 0:
        dist = "prob_dist_non_match"

    levels = params["π"][gamma_str][dist]

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


def get_gamma_cols_from_df(df):
    cols = df.columns
    gamma_cols = [c for c in cols if re.match("gamma_\d", c)]
    return gamma_cols


def sql_gen_gamma_prob_columns(df_with_gamma, params):
    """
    For each row, look up the probability of observing the gamma value given the record is a match and non_match respectively
    """

    gamma_cols = get_gamma_cols_from_df(df_with_gamma)

    gamma_cols_to_select = ", ".join(gamma_cols)

    case_statements = []
    for gamma_str in gamma_cols:
        for match in [0, 1]:
            case_statements.append(
                sql_gen_gamma_case_when(gamma_str, match, params))

    case_statements = ", \n\n".join(case_statements)
    df_with_gamma.registerTempTable("df_with_gamma")
    sql = f"""
    select
    {gamma_cols_to_select},
    {case_statements}
    from df_with_gamma
    """

    return sql


def sql_gen_expected_match_prob(df, params):
    gamma_cols = get_gamma_cols_from_df(df)
    numerator = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    denom_part = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])
    match_prob_expression = f"({params['λ']} * {numerator})/(( {params['λ']} * {numerator}) + (({1 -params['λ']} ) * {denom_part})) as match_probability"

    sql = f"""
    select *,
    {match_prob_expression}
    from df_with_gamma_probs
    """

    return sql


def run_expectation_step(df_with_gamma, spark, params):
    df_with_gamma.registerTempTable("df_with_gamma")

    sql = sql_gen_gamma_prob_columns(df_with_gamma, params)
    df_with_gamma_probs = spark.sql(sql)
    log.debug(sql)
    df_with_gamma_probs.registerTempTable("df_with_gamma_probs")

    sql = sql_gen_expected_match_prob(df_with_gamma_probs, params)
    df_e = spark.sql(sql)
    log.debug(sql)
    df_e.registerTempTable("df_e")
    return df_e


def get_new_lambda(df_e, spark):
    df_e.registerTempTable("df_e")
    sql = """
    select cast(sum(match_probability)/count(*) as float) as new_lambda
    from df_e
    """
    new_lambda = spark.sql(sql).collect()[0][0]
    log.debug(sql)
    return new_lambda


def sql_gen_intermediate_pi_aggregate(df):
    gamma_cols = get_gamma_cols_from_df(df)

    gamma_cols_expr = ", ".join(gamma_cols)

    sql = f"""
    select {gamma_cols_expr}, sum(match_probability) as expected_num_matches, sum(1- match_probability) as expected_num_non_matches, count(*) as num_rows
    from df_e
    group by {gamma_cols_expr}

    """
    return sql


def value_to_level(gamma_str, value, params):

    for level_key, level in params["π"][gamma_str]["prob_dist_match"].items():
        if level["value"] == value:
            return level_key


def get_new_pi_for_single_gamma(df_e, spark, gamma_cols, params):

    sqls = []

    for gamma_str in gamma_cols:
        sql = f"""
        select {gamma_str} as gamma_value,
        cast(sum(expected_num_matches)/(select sum(expected_num_matches) from df_intermediate) as float) as new_probability_match,
        cast(sum(expected_num_non_matches)/(select sum(expected_num_non_matches) from df_intermediate) as float) as new_probability_non_match,
        '{gamma_str}' as gamma_col
        from df_intermediate
        group by {gamma_str}
        """
        sqls.append(sql)

    sql = "\nunion\n".join(sqls)

    levels = spark.sql(sql).collect()
    log.debug(sql)

    for l in levels:
        row_dict = l.asDict()

        gamma_value = row_dict["gamma_value"]
        gamma_str = row_dict["gamma_col"]
        level = value_to_level(gamma_str, gamma_value, params)
        new_prob_match = row_dict["new_probability_match"]

        new_prob_non_match = row_dict["new_probability_non_match"]

        params["π"][gamma_str]["prob_dist_match"][level]["probability"] = new_prob_match
        params["π"][gamma_str]["prob_dist_non_match"][level]["probability"] = new_prob_non_match

    return params


def update_params(df_e, spark, params):

    new_params = copy.deepcopy(params)
    gamma_cols = get_gamma_cols_from_df(df_e)

    ## Reset params
    new_params["λ"] = None
    for gamma_str in gamma_cols:
        for level_key, level_value in new_params["π"][gamma_str]["prob_dist_match"].items():
            level_value["probability"] = None
        for level_key, level_value in new_params["π"][gamma_str]["prob_dist_non_match"].items():
            level_value["probability"] = None

    new_lambda = get_new_lambda(df_e, spark)
    new_params["λ"] = new_lambda

    sql = sql_gen_intermediate_pi_aggregate(df_e)
    df_intermediate = spark.sql(sql)
    log.debug(sql)
    df_intermediate.registerTempTable("df_intermediate")
    df_intermediate.persist()

    new_params = get_new_pi_for_single_gamma(
        df_e, spark, gamma_cols, new_params)

    return new_params

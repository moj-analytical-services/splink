"""
In the expectation step we calculate the membership probabilities
i.e. for each comparison, what is the probability that it's a member
of group match = 0 and group match = 1
"""
from .logging_utils import _format_sql

import logging
from collections import OrderedDict

# For type hints. Try except to ensure the sql_gen functions even if spark doesn't exist.
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

logger = logging.getLogger(__name__)

from .gammas import _add_left_right
from .params import Params
from .check_types import check_types

@check_types
def run_expectation_step(df_with_gamma: DataFrame,
                         params: Params,
                         settings: dict,
                         spark: SparkSession,
                         compute_ll=False):
    """Run the expectation step of the EM algorithm described in the fastlink paper:
    http://imai.fas.harvard.edu/research/files/linkage.pdf

      Args:
          df_with_gamma (DataFrame): Spark dataframe with comparison vectors already populated
          params (Params): splink params object
          settings (dict): splink settings dictionary
          spark (SparkSession): SparkSession
          compute_ll (bool, optional): Whether to compute the log likelihood. Degrades performance. Defaults to False.

      Returns:
          DataFrame: Spark dataframe with a match_probability column
      """


    sql = _sql_gen_gamma_prob_columns(params, settings)

    df_with_gamma.createOrReplaceTempView("df_with_gamma")
    logger.debug(_format_sql(sql))
    df_with_gamma_probs = spark.sql(sql)
    
    # This is optional because is slows down execution
    if compute_ll:
        ll = get_overall_log_likelihood(df_with_gamma_probs, params, spark)
        message = f"Log likelihood for iteration {params.iteration-1}:  {ll}"
        logger.info(message)
        params.params["log_likelihood"] = ll

    sql = _sql_gen_expected_match_prob(params, settings)

    logger.debug(_format_sql(sql))
    df_with_gamma_probs.createOrReplaceTempView("df_with_gamma_probs")
    df_e = spark.sql(sql)

    df_e.createOrReplaceTempView("df_e")
    return df_e


def _sql_gen_gamma_prob_columns(params, settings, table_name="df_with_gamma"):
    """
    For each row, look up the probability of observing the gamma value given the record
    is a match and non_match respectively
    """

    # Get case statements
    case_statements = {}
    for gamma_str in params._gamma_cols:
        for match in [0, 1]:
            alias = _case_when_col_alias(gamma_str, match)
            case_statement = _sql_gen_gamma_case_when(gamma_str, match, params)
            case_statements[alias] = case_statement


    # Column order for case statement.  We want orig_col_l, orig_col_r, gamma_orig_col, prob_gamma_u, prob_gamma_m
    select_cols = OrderedDict()
    select_cols = _add_left_right(select_cols, settings["unique_id_column_name"])

    for col in settings["comparison_columns"]:
        if "col_name" in col:
            col_name = col["col_name"]
            if settings["retain_matching_columns"]:
                select_cols = _add_left_right(select_cols, col_name)
            if col["term_frequency_adjustments"]:
                select_cols = _add_left_right(select_cols, col_name)

            select_cols["gamma_" + col_name] = "gamma_" + col_name

        if "custom_name" in col:
            col_name = col["custom_name"]

            if settings["retain_matching_columns"]:
                for c2 in col["custom_columns_used"]:
                    select_cols = _add_left_right(select_cols, c2)

            select_cols["gamma_" + col_name] = "gamma_" + col_name

        select_cols[f"prob_gamma_{col_name}_non_match"] = case_statements[f"prob_gamma_{col_name}_non_match"]
        select_cols[f"prob_gamma_{col_name}_match"] = case_statements[f"prob_gamma_{col_name}_match"]

    if settings["link_type"] == 'link_and_dedupe':
        select_cols = _add_left_right(select_cols, "_source_table")

    for c in settings["additional_columns_to_retain"]:
        select_cols = _add_left_right(select_cols, c)

    select_expr =  ", ".join(select_cols.values())


    sql = f"""
    -- We use case statements for these lookups rather than joins for performance and simplicity
    select {select_expr}
    from {table_name}
    """

    return sql


def _column_order_df_e_select_expr(settings, tf_adj_cols=False):
    # Column order for case statement.  We want orig_col_l, orig_col_r, gamma_orig_col, prob_gamma_u, prob_gamma_m
    select_cols = OrderedDict()
    select_cols = _add_left_right(select_cols, settings["unique_id_column_name"])

    for col in settings["comparison_columns"]:
        if "col_name" in col:
            col_name = col["col_name"]

            # Note adding cols is idempotent so don't need to worry about adding twice
            if settings["retain_matching_columns"]:
                select_cols = _add_left_right(select_cols, col_name)
            if col["term_frequency_adjustments"]:
                select_cols = _add_left_right(select_cols, col_name)
            select_cols["gamma_" + col_name] = "gamma_" + col_name

        if "custom_name" in col:
            col_name = col["custom_name"]
            if settings["retain_matching_columns"]:
                for c2 in col["custom_columns_used"]:
                    select_cols = _add_left_right(select_cols, c2)
            select_cols["gamma_" + col_name] = "gamma_" + col_name

        if settings["retain_intermediate_calculation_columns"]:
            select_cols[f"prob_gamma_{col_name}_non_match"] = f"prob_gamma_{col_name}_non_match"
            select_cols[f"prob_gamma_{col_name}_match"] = f"prob_gamma_{col_name}_match"
            if tf_adj_cols:
                if col["term_frequency_adjustments"]:
                    select_cols[col_name+"_tf_adj"] =  col_name+"_tf_adj"



    if settings["link_type"] == 'link_and_dedupe':
        select_cols = _add_left_right(select_cols, "_source_table")

    for c in settings["additional_columns_to_retain"]:
        select_cols = _add_left_right(select_cols, c)
    return ", ".join(select_cols.values())

def _sql_gen_expected_match_prob(params, settings, table_name="df_with_gamma_probs"):
    gamma_cols = params._gamma_cols

    numerator = " * ".join([f"prob_{g}_match" for g in gamma_cols])
    denom_part = " * ".join([f"prob_{g}_non_match" for g in gamma_cols])

    λ = params.params['λ']
    castλ = f"cast({λ} as double)"
    castoneminusλ = f"cast({1-λ} as double)"
    match_prob_expression = f"({castλ} * {numerator})/(( {castλ} * {numerator}) + ({castoneminusλ} * {denom_part})) as match_probability"

    select_expr = _column_order_df_e_select_expr(settings)

    sql = f"""
    select {match_prob_expression}, {select_expr}
    from {table_name}
    """

    return sql

def _case_when_col_alias(gamma_str, match):

    if match == 1:
        name_suffix = "_match"
    if match == 0:
        name_suffix = "_non_match"

    return f"prob_{gamma_str}{name_suffix}"

def _sql_gen_gamma_case_when(gamma_str, match, params):
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
    case_statements.append(f"WHEN {gamma_str} = -1 THEN cast(1 as double)")
    for level in levels.values():
            case_stmt = f"when {gamma_str} = {level['value']} then cast({level['probability']:.35f} as double)"
            case_statements.append(case_stmt)

    case_statements = "\n".join(case_statements)

    alias = _case_when_col_alias(gamma_str, match)

    sql = f""" case \n{case_statements} \nend \nas {alias}"""

    return sql.strip()


def _calculate_log_likelihood_df(df_with_gamma_probs, params, spark):
    """
    Compute likelihood of observing df_with_gamma given the parameters

    Likelihood is just ((1-lambda) * prob not match) * (lambda * prob match)
    """

    gamma_cols = params._gamma_cols

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
    cast({log_likelihood} as double) as  log_likelihood,
    {match_prob_expression}

    from df_with_gamma_probs
    """
    logger.debug(_format_sql(sql))
    df = spark.sql(sql)

    return df


def get_overall_log_likelihood(df_with_gamma_probs, params, spark):
    """Compute overall log likelihood score for model

    Args:
        df_with_gamma_probs (DataFrame): A dataframe of comparisons with corresponding probabilities
        params (Params): splink Params object
        spark (SparkSession): Your sparksession.

    Returns:
        float: The log likelihood
    """

    df = _calculate_log_likelihood_df(df_with_gamma_probs, params, spark)
    return df.groupby().sum("log_likelihood").collect()[0][0]

import logging

from typeguard import typechecked

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from .logging_utils import _format_sql

from .gammas import (
    _add_left_right,
    _retain_source_dataset_column,
    _add_unique_id_and_source_dataset,
)
from .model import Model
from .ordered_set import OrderedSet
from .settings import ComparisonColumn

logger = logging.getLogger(__name__)


@typechecked
def run_expectation_step(
    df_with_gamma: DataFrame,
    model: Model,
    spark: SparkSession,
    compute_ll=False,
):
    """Run the expectation step of the EM algorithm described in the fastlink paper:
    http://imai.fas.harvard.edu/research/files/linkage.pdf

      Args:
          df_with_gamma (DataFrame): Spark dataframe with comparison vectors already populated
          model (Model): splink Model object
          spark (SparkSession): SparkSession
          compute_ll (bool, optional): Whether to compute the log likelihood. Degrades performance. Defaults to False.

      Returns:
          DataFrame: Spark dataframe with a match_probability column
    """

    retain_source_dataset = _retain_source_dataset_column(
        model.current_settings_obj.settings_dict, df_with_gamma
    )

    sql = _sql_gen_gamma_prob_columns(model, retain_source_dataset)

    df_with_gamma.createOrReplaceTempView("df_with_gamma")
    logger.debug(_format_sql(sql))
    df_with_gamma_probs = spark.sql(sql)

    # This is optional because is slows down execution
    if compute_ll:
        ll = get_overall_log_likelihood(df_with_gamma_probs, model, spark)
        message = f"Log likelihood for iteration {model.iteration-1}:  {ll}"
        logger.info(message)
        model.current_settings_obj["log_likelihood"] = ll

    sql = _sql_gen_expected_match_prob(model, retain_source_dataset)

    logger.debug(_format_sql(sql))
    df_with_gamma_probs.createOrReplaceTempView("df_with_gamma_probs")
    df_e = spark.sql(sql)

    df_e.createOrReplaceTempView("df_e")

    model.save_settings_to_iteration_history()

    return df_e


def _sql_gen_gamma_prob_columns(
    model: Model, retain_source_dataset_col: bool, table_name="df_with_gamma"
):
    """
    For each row, look up the probability of observing the gamma value given the record
    is a match and non_match respectively
    """
    settings = model.current_settings_obj.settings_dict

    # Dictionary of case statements - these will be used in the list of columsn
    # in the SQL 'select' statement
    case_statements = {}
    for cc in model.current_settings_obj.comparison_columns_list:
        for match in [0, 1]:
            alias = _case_when_col_alias(cc.gamma_name, match)
            case_statement = _sql_gen_gamma_case_when(cc, match)
            case_statements[alias] = case_statement

    select_cols = OrderedSet()
    uid = settings["unique_id_column_name"]
    sds = settings["source_dataset_column_name"]
    select_cols = _add_unique_id_and_source_dataset(
        select_cols, uid, sds, retain_source_dataset_col
    )

    for col in settings["comparison_columns"]:
        cc = ComparisonColumn(col)
        if settings["retain_matching_columns"]:
            for col_name in cc.columns_used:
                select_cols = _add_left_right(select_cols, col_name)
        if col["term_frequency_adjustments"]:
            select_cols = _add_left_right(select_cols, cc.name)

        select_cols.add("gamma_" + cc.name)
        select_cols.add(case_statements[f"prob_gamma_{cc.name}_non_match"])
        select_cols.add(case_statements[f"prob_gamma_{cc.name}_match"])

    for c in settings["additional_columns_to_retain"]:
        select_cols = _add_left_right(select_cols, c)

    if "blocking_rules" in settings:
        if len(settings["blocking_rules"]) > 1:
            select_cols.add("match_key")

    select_expr = ", ".join(select_cols)

    sql = f"""
    -- We use case statements for these lookups rather than joins for performance and simplicity
    select {select_expr}
    from {table_name}
    """

    return sql


def _column_order_df_e_select_expr(
    settings, retain_source_dataset_col, tf_adj_cols=False
):
    # Column order for case statement.  We want orig_col_l, orig_col_r, gamma_orig_col, prob_gamma_u, prob_gamma_m
    select_cols = OrderedSet()
    uid = settings["unique_id_column_name"]
    sds = settings["source_dataset_column_name"]
    select_cols = _add_unique_id_and_source_dataset(
        select_cols, uid, sds, retain_source_dataset_col
    )

    for col in settings["comparison_columns"]:

        cc = ComparisonColumn(col)
        if settings["retain_matching_columns"]:
            for col_name in cc.columns_used:
                select_cols = _add_left_right(select_cols, col_name)
        if col["term_frequency_adjustments"]:
            select_cols = _add_left_right(select_cols, cc.name)

        select_cols.add("gamma_" + cc.name)

        if settings["retain_intermediate_calculation_columns"]:
            select_cols.add(f"prob_gamma_{cc.name}_non_match")
            select_cols.add(f"prob_gamma_{cc.name}_match")

            if tf_adj_cols:
                if col["term_frequency_adjustments"]:
                    select_cols.add(cc.name + "_tf_adj")

    for c in settings["additional_columns_to_retain"]:
        select_cols = _add_left_right(select_cols, c)

    if "blocking_rules" in settings:
        if len(settings["blocking_rules"]) > 1:
            select_cols.add("match_key")
    return ", ".join(select_cols)


def _sql_gen_expected_match_prob(
    model, retain_source_dataset, table_name="df_with_gamma_probs"
):
    settings = model.current_settings_obj.settings_dict
    ccs = model.current_settings_obj.comparison_columns_list

    numerator = " * ".join([f"prob_{cc.gamma_name}_match" for cc in ccs])
    denom_part = " * ".join([f"prob_{cc.gamma_name}_non_match" for cc in ccs])

    λ = model.current_settings_obj["proportion_of_matches"]
    castλ = f"cast({λ} as double)"
    castoneminusλ = f"cast({1-λ} as double)"
    match_prob_expression = f"({castλ} * {numerator})/(( {castλ} * {numerator}) + ({castoneminusλ} * {denom_part})) as match_probability"

    select_expr = _column_order_df_e_select_expr(settings, retain_source_dataset)

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


def _sql_gen_gamma_case_when(comparison_column, match):
    """
    Create the case statements that look up the correct probabilities in the
    model dict for each gamma
    """
    cc = comparison_column

    if match == 1:
        probs = cc["m_probabilities"]
    if match == 0:
        probs = cc["u_probabilities"]

    case_statements = []
    case_statements.append(f"WHEN {cc.gamma_name} = -1 THEN cast(1 as double)")

    for gamma_index, prob in enumerate(probs):
        case_stmt = (
            f"when {cc.gamma_name} = {gamma_index} then cast({prob:.35f} as double)"
        )
        case_statements.append(case_stmt)

    case_statements = "\n".join(case_statements)

    alias = _case_when_col_alias(cc.gamma_name, match)

    sql = f""" case \n{case_statements} \nend \nas {alias}"""

    return sql.strip()


def _calculate_log_likelihood_df(df_with_gamma_probs, model, spark):
    """
    Compute likelihood of observing df_with_gamma given the parameters

    Likelihood is just ((1-lambda) * prob not match) * (lambda * prob match)
    """

    cc = model.current_settings_obj.comparison_columns_list
    λ = model.current_settings_obj["proportion_of_matches"]

    match_prob = " * ".join([f"prob_{c.gamma_name}_match" for c in cc])
    match_prob = f"({λ} * {match_prob})"
    non_match_prob = " * ".join([f"prob_{c.gamma_name}_non_match" for c in cc])
    non_match_prob = f"({1-λ} * {non_match_prob})"
    log_likelihood = f"ln({match_prob} + {non_match_prob})"

    numerator = " * ".join([f"prob_{c.gamma_name}_match" for c in cc])
    denom_part = " * ".join([f"prob_{c.gamma_name}_non_match" for c in cc])
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


def get_overall_log_likelihood(df_with_gamma_probs, model, spark):
    """Compute overall log likelihood score for model

    Args:
        df_with_gamma_probs (DataFrame): A dataframe of comparisons with corresponding probabilities
        model (Model): splink Model object
        spark (SparkSession): Your sparksession.

    Returns:
        float: The log likelihood
    """

    df = _calculate_log_likelihood_df(df_with_gamma_probs, model, spark)
    return df.groupby().sum("log_likelihood").collect()[0][0]

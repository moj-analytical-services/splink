# For more information on where formulas came from, see
# https://github.com/moj-analytical-services/splink/pull/107

import logging
import math
import warnings
from copy import deepcopy

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from .logging_utils import _format_sql
from .expectation_step import _column_order_df_e_select_expr
from .model import Model
from .maximisation_step import run_maximisation_step
from .gammas import _retain_source_dataset_column
from .settings import Settings
from typeguard import typechecked

logger = logging.getLogger(__name__)


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


# See https://github.com/moj-analytical-services/splink/pull/107
def sql_gen_generate_adjusted_lambda(column_name, model, table_name="df_e"):

    # Get 'average' param for matching on this column
    cc = model.current_settings_obj.get_comparison_column(column_name)
    max_level = cc.max_gamma_index

    m = cc["m_probabilities"][max_level]
    u = cc["u_probabilities"][max_level]

    # ensure average adj calculation doesnt divide by zero (see issue 118)

    is_none = m is None or u is None

    no_adjust = is_none or math.isclose((m + u), 0.0, rel_tol=1e-9, abs_tol=0.0)

    if no_adjust:
        average_adjustment = 0.5
        warnings.warn(
            f"There were no comparisons in column {column_name} which were in the highest level of similarity, so no adjustment could be made"
        )

    else:
        average_adjustment = m / (m + u)

    sql = f"""
    with temp_adj as
    (
    select {column_name}_l, sum(1-match_probability)/(select sum(1-match_probability) from df_e) as u, sum(match_probability)/(select sum(match_probability) from df_e) as m
    from {table_name}
    where {column_name}_l = {column_name}_r
    group by {column_name}_l
    )

    select {column_name}_l, {sql_gen_bayes_string(["(m/(m+u))", 1-average_adjustment])}
    as {column_name}_tf_adj_nulls
    from temp_adj
    """

    return sql


def sql_gen_add_adjumentments_to_df_e(term_freq_column_list):

    coalesce_template = "coalesce({c}_tf_adj_nulls, 0.5) as {c}_tf_adj"
    coalesces = [coalesce_template.format(c=c) for c in term_freq_column_list]
    coalesces = ",\n ".join(coalesces)

    left_join_template = """
     left join
    {c}_lookup
    on {c}_lookup.{c}_l = e.{c}_l and {c}_lookup.{c}_l = e.{c}_r
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


def sql_gen_compute_final_group_membership_prob_from_adjustments(
    term_freq_column_list, settings, retain_source_dataset_col, table_name="df_e_adj"
):

    term_freq_column_list = [c + "_tf_adj" for c in term_freq_column_list]
    term_freq_column_list.insert(0, "match_probability")
    tf_adjusted_match_prob_expr = sql_gen_bayes_string(term_freq_column_list)

    select_expr = _column_order_df_e_select_expr(
        settings, retain_source_dataset_col, tf_adj_cols=True
    )

    sql = f"""
    select
        {tf_adjusted_match_prob_expr} as tf_adjusted_match_prob,
        match_probability,
        {select_expr}

    from {table_name}
    """

    return sql


@typechecked
def make_adjustment_for_term_frequencies(
    df_e: DataFrame,
    model: Model,
    spark: SparkSession,
    retain_adjustment_columns: bool = False,
):

    # Running a maximisation step will eliminate errors cause by global parameters
    # being used in blocked jobs

    settings = model.current_settings_obj.settings_dict

    term_freq_column_list = [
        cc.name
        for cc in model.current_settings_obj.comparison_columns_list
        if cc["term_frequency_adjustments"] is True
    ]

    if len(term_freq_column_list) == 0:
        return df_e

    retain_source_dataset_col = _retain_source_dataset_column(settings, df_e)
    df_e.createOrReplaceTempView("df_e")

    old_settings = deepcopy(model.current_settings_obj.settings_dict)

    for cc in model.current_settings_obj.comparison_columns_list:
        cc.column_dict["fix_m_probabilities"] = False
        cc.column_dict["fix_u_probabilities"] = False

    run_maximisation_step(df_e, model, spark)

    # Generate a lookup table for each column with 'term specific' lambdas.
    for c in term_freq_column_list:
        sql = sql_gen_generate_adjusted_lambda(c, model)
        logger.debug(_format_sql(sql))
        lookup = spark.sql(sql)
        lookup.persist()
        lookup.createOrReplaceTempView(f"{c}_lookup")

    # Merge these lookup tables into main table
    sql = sql_gen_add_adjumentments_to_df_e(term_freq_column_list)
    logger.debug(_format_sql(sql))
    df_e_adj = spark.sql(sql)
    df_e_adj.createOrReplaceTempView("df_e_adj")

    sql = sql_gen_compute_final_group_membership_prob_from_adjustments(
        term_freq_column_list, settings, retain_source_dataset_col
    )
    logger.debug(_format_sql(sql))
    df = spark.sql(sql)
    if not retain_adjustment_columns:
        for c in term_freq_column_list:
            df = df.drop(c + "_tf_adj")

    # Restore original settings
    model.current_settings_obj.settings_dict = old_settings

    return df

#############################
# NEW TF ADJUSTMENT METHOD
#############################

from .settings import complete_settings_dict


def sql_gen_term_frequencies(column_name, table_name="df"):

    sql = f"""
    select
    {column_name}, count(*) / sum(count(*)) over () as tf_{column_name}
    from {table_name}
    where {column_name} is not null
    group by {column_name}
    """

    return sql


def _sql_gen_add_term_frequencies(
    settings: dict,
    table_name: str = "df"
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings (dict): `splink` settings dict
        table_name (str, optional): Name of the source df. Defaults to "df".

    Returns:
        str: A SQL string
    """

    cols = [cc["col_name"] for cc in settings["comparison_columns"] if cc["term_frequency_adjustments"]]
    tf_tables = ", ".join([f"tf_{col} as ({sql_gen_term_frequencies(col, table_name)})" for col in cols])

    tf_cols = ", ".join(f"tf_{col}.tf_{col}" for col in cols) 

    joins = "".join([f"""
    left join tf_{col} 
    on {table_name}.{col} = tf_{col}.{col}
    """ for col in cols])

    sql = f"""
    with {tf_tables}
    select {table_name}.*, {tf_cols}
    from {table_name}
    {joins}
    """

    return sql


def add_term_frequencies(
    df: DataFrame,
    settings_dict: dict,
    spark: SparkSession
):
    """Compute the term frequencies of the required columns and add to the dataframe.  
    Args:
        df (spark dataframe): A Spark dataframe containing source records for linking
        settings_dict (dict): The `splink` settings dictionary
        spark (Spark session): The Spark session object

    Returns:
        Spark dataframe: A dataframe containing new columns representing the term frequencies
        of the corresponding values
    """

    settings_dict = complete_settings_dict(settings_dict, spark)

    sql = _sql_gen_add_term_frequencies(settings_dict, "df")

    logger.debug(_format_sql(sql))
    df.createOrReplaceTempView("df")
    df_with_tf = spark.sql(sql)

    return df_with_tf

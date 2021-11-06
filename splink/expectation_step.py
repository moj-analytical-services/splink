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
def run_expectation_step(df_with_gamma: DataFrame, model: Model, spark: SparkSession):
    """Run the expectation step of the EM algorithm described in the fastlink paper:
    http://imai.fas.harvard.edu/research/files/linkage.pdf

      Args:
          df_with_gamma (DataFrame): Spark dataframe with comparison vectors already populated
          model (Model): splink Model object
          spark (SparkSession): SparkSession

      Returns:
          DataFrame: Spark dataframe with a match_probability column
    """

    retain_source_dataset = _retain_source_dataset_column(
        model.current_settings_obj.settings_dict, df_with_gamma
    )

    sql = _sql_gen_gamma_bf_columns(model, retain_source_dataset)

    df_with_gamma.createOrReplaceTempView("df_with_gamma")
    logger.debug(_format_sql(sql))
    df_with_gamma_probs = spark.sql(sql)

    sql = _sql_gen_expected_match_prob(model, retain_source_dataset)

    logger.debug(_format_sql(sql))
    df_with_gamma_probs.createOrReplaceTempView("df_with_gamma_probs")
    df_e = spark.sql(sql)

    df_e.createOrReplaceTempView("df_e")

    model.save_settings_to_iteration_history()

    return df_e


def _sql_gen_gamma_bf_columns(
    model: Model, retain_source_dataset_col: bool, table_name="df_with_gamma"
):
    """
    For each row, look up the bayes factor associated with the observed
    gamma value
    """
    settings = model.current_settings_obj.settings_dict

    # Dictionary of case statements - these will be used in the list of columsn
    # in the SQL 'select' statement
    case_statements = {}
    for cc in model.current_settings_obj.comparison_columns_list:
        case_statements[f"bf_gamma_{cc.name}"] = _sql_gen_bayes_factors(cc)
        if cc.term_frequency_adjustments:
            case_statements[f"bf_tf_adj_{cc.name}"] = _sql_gen_bayes_factors(
                cc, tf_adj=True
            )

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
            select_cols.add(case_statements[f"bf_tf_adj_{cc.name}"])

        select_cols.add("gamma_" + cc.name)

        select_cols.add(case_statements[f"bf_gamma_{cc.name}"])

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
    # Column order for case statement.  We want orig_col_l, orig_col_r, gamma_orig_col,
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

        select_cols.add(f"gamma_{cc.name}")

        if settings["retain_intermediate_calculation_columns"]:

            select_cols.add(f"bf_gamma_{cc.name}")

            if tf_adj_cols:
                if col["term_frequency_adjustments"]:
                    select_cols.add(f"bf_tf_adj_{cc.name}")

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

    λ = model.current_settings_obj["proportion_of_matches"]

    bf_cols = [f"bf_{cc.gamma_name}" for cc in ccs]
    bf_tf_adj_cols = [
        f"bf_tf_adj_{cc.name}" for cc in ccs if cc.term_frequency_adjustments
    ]
    bf_cols.extend(bf_tf_adj_cols)

    bayes_factor = " * ".join(bf_cols)
    bayes_factor = f"{λ}D/ (1-{λ}D) * {bayes_factor}"

    select_expr = _column_order_df_e_select_expr(
        settings, retain_source_dataset, tf_adj_cols=True
    )

    sql = f"""
    with df_with_overall_bf as (
        select
            *,
            {bayes_factor} as __bf
        from {table_name} )

    select
        log2(__bf) as match_weight,
        (__bf/(1+__bf)) as match_probability,
        {select_expr}
    from df_with_overall_bf
    """

    return sql


def _sql_gen_bayes_factors(comparison_column, tf_adj=False):
    """
    Create the case statements that look up the correct probabilities in the
    model dict for each gamma to calculate Bayes factors (m / u) and additional
    term frequency Bayes factors (u / (term frequency))
    """
    cc = comparison_column

    case_statements = []
    case_statements.append(f"when {cc.gamma_name} = -1 then 1.0D")

    if not tf_adj:
        alias = f"bf_{cc.gamma_name}"
        # This will fail if any of the values are null
        # If the values are null, this means that the value never occurs in the dataset
        # We can set the bf to null in this case

        m_u_zip = zip(cc["m_probabilities"], cc["u_probabilities"])

        for gamma_index, (m, u) in enumerate(m_u_zip):
            if m is None or u is None:
                case_stmt = f"when {cc.gamma_name} = {gamma_index} then null"
            else:
                case_stmt = f"when {cc.gamma_name} = {gamma_index} then {m/u}D"

            case_statements.append(case_stmt)
    else:
        alias = f"bf_tf_adj_{cc.name}"

        # The full weight tf adjustment is based on an exact match
        # i.e. it's computed based on how likely tokens are
        # to collide. relative to average
        # The adjustment is based on weight this adjustment down

        u_prob_exact = cc["u_probabilities"][-1]
        tf_weights = cc["tf_adjustment_weights"]

        case_stmt = f"when tf_{cc.name}_l is null or tf_{cc.name}_r is null then null"
        case_statements.append(case_stmt)

        for gamma_index, weight in enumerate(tf_weights):
            if u_prob_exact is not None:
                bf_tf = f"{u_prob_exact}D / greatest(tf_{cc.name}_l, tf_{cc.name}_r)"
                case_stmt = f"when {cc.gamma_name} = {gamma_index} then power({bf_tf}, {weight}D)"
            else:
                case_stmt = f"when {cc.gamma_name} = {gamma_index} then 1"

            case_statements.append(case_stmt)

    case_statements = "\n".join(case_statements)

    sql = f""" case
        {case_statements}
        end
        as {alias}
        """

    return sql.strip()

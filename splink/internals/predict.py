from __future__ import annotations

# This is otherwise known as the expectation step of the EM algorithm.
import logging
import math
from typing import List

from splink.internals.comparison import Comparison
from splink.internals.dialects import SplinkDialect
from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    threshold_args_to_match_weight,
)

from .settings import CoreModelSettings, Settings

logger = logging.getLogger(__name__)

# Constants for clamping to prevent numerical instability
PROB_CLAMP_MIN = 1e-10  # Minimum probability (prevents log(0) for prior)
PROB_CLAMP_MAX = 1 - 1e-10  # Maximum probability (prevents log(inf) for prior)


def predict_from_comparison_vectors_sqls_using_settings(
    settings_obj: Settings,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    include_clerical_match_score: bool = False,
) -> list[dict[str, str]]:
    """Generate SQL for predicting match weights and probabilities.

    Uses numerically stable additive match weights.

    Args:
        settings_obj: The Settings object containing model configuration
        threshold_match_probability: Minimum match probability to return
        threshold_match_weight: Minimum match weight to return
        include_clerical_match_score: Whether to include clerical match score column

    Returns:
        List of SQL info dictionaries
    """
    return predict_from_comparison_vectors_sqls_additive(
        unique_id_input_columns=settings_obj.column_info_settings.unique_id_input_columns,
        core_model_settings=settings_obj.core_model_settings,
        sql_dialect=SplinkDialect.from_string(settings_obj._sql_dialect_str),
        threshold_match_probability=threshold_match_probability,
        threshold_match_weight=threshold_match_weight,
        retain_matching_columns=settings_obj._retain_matching_columns,
        retain_intermediate_calculation_columns=settings_obj._retain_intermediate_calculation_columns,
        training_mode=False,
        additional_columns_to_retain=settings_obj._additional_columns_to_retain,
        include_clerical_match_score=include_clerical_match_score,
    )


def predict_from_comparison_vectors_sqls_additive(
    unique_id_input_columns: List[InputColumn],
    core_model_settings: CoreModelSettings,
    sql_dialect: SplinkDialect,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    # by default we keep off everything we don't necessarily need
    retain_matching_columns: bool = False,
    retain_intermediate_calculation_columns: bool = False,
    training_mode: bool = False,
    additional_columns_to_retain: List[InputColumn] = [],
    include_clerical_match_score: bool = False,
) -> list[dict[str, str]]:
    """Generate prediction SQL using additive match weights.

    This function generates SQL that computes match weights by summing
    partial match weights (log2 of Bayes factors) directly, rather than
    multiplying Bayes factors and taking the log afterward.

    This approach is numerically stable and avoids log(0) errors.
    """
    sqls = []

    select_cols = Settings.columns_to_select_for_match_weight_parts(
        unique_id_input_columns=unique_id_input_columns,
        comparisons=core_model_settings.comparisons,
        retain_matching_columns=retain_matching_columns,
        retain_intermediate_calculation_columns=retain_intermediate_calculation_columns,
        additional_columns_to_retain=additional_columns_to_retain,
    )
    select_cols_expr = ",".join(select_cols)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    sql = f"""
    select {select_cols_expr} {clerical_match_score}
    from __splink__df_comparison_vectors
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_match_weight_parts",
    }
    sqls.append(sql_info)

    select_cols = Settings.columns_to_select_for_predict(
        unique_id_input_columns=unique_id_input_columns,
        comparisons=core_model_settings.comparisons,
        retain_matching_columns=retain_matching_columns,
        retain_intermediate_calculation_columns=retain_intermediate_calculation_columns,
        training_mode=training_mode,
        additional_columns_to_retain=additional_columns_to_retain,
    )
    select_cols_expr = ",".join(select_cols)

    # Collect match weight column names to sum
    mw_terms = []
    for cc in core_model_settings.comparisons:
        mw_terms.extend(cc._match_weight_columns_to_sum)

    prior = core_model_settings.probability_two_random_records_match
    match_weight_expr, match_prob_expr = _combine_prior_and_mws(
        prior,
        mw_terms,
        sql_dialect,
    )

    threshold_as_mw = threshold_args_to_match_weight(
        threshold_match_probability, threshold_match_weight
    )

    if threshold_as_mw is not None:
        threshold_expr = f" where ({match_weight_expr}) >= {threshold_as_mw} "
    else:
        threshold_expr = ""

    sql = f"""
    select
    ({match_weight_expr}) as match_weight,
    {match_prob_expr} as match_probability,
    {select_cols_expr} {clerical_match_score}
    from __splink__df_match_weight_parts
    {threshold_expr}
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_predict",
    }
    sqls.append(sql_info)

    return sqls


def predict_from_agreement_pattern_counts_sqls(
    comparisons: List[Comparison],
    probability_two_random_records_match: float,
    sql_dialect: SplinkDialect,
) -> list[dict[str, str]]:
    """Generate prediction SQL from agreement pattern counts using additive match weights.

    This is used during EM training when estimate_without_term_frequencies=True.
    Uses the numerically stable additive match weight approach.
    """
    sqls = []

    select_cols = []

    for cc in comparisons:
        cc_sqls = [
            cl._match_weight_sql(cc._gamma_column_name) for cl in cc.comparison_levels
        ]
        sql = " ".join(cc_sqls)
        sql = f"CASE {sql} END as {cc._mw_column_name}"
        select_cols.append(cc._gamma_column_name)
        select_cols.append(sql)
    select_cols.append("agreement_pattern_count")
    select_cols_expr = ",".join(select_cols)

    sql = f"""
    select {select_cols_expr}
    from __splink__agreement_pattern_counts
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_match_weight_parts",
    }
    sqls.append(sql_info)

    select_cols = []
    for cc in comparisons:
        select_cols.append(cc._gamma_column_name)
        select_cols.append(cc._mw_column_name)
    select_cols.append("agreement_pattern_count")
    select_cols_expr = ",".join(select_cols)

    prior = probability_two_random_records_match
    mw_terms = [cc._mw_column_name for cc in comparisons]
    match_weight_expr, match_prob_expr = _combine_prior_and_mws(
        prior,
        mw_terms,
        sql_dialect,
    )

    sql = f"""
    select
    ({match_weight_expr}) as match_weight,
    {match_prob_expr} as match_probability,
    {select_cols_expr}
    from __splink__df_match_weight_parts
    """

    sql_info = {
        "sql": sql,
        "output_table_name": "__splink__df_predict",
    }
    sqls.append(sql_info)

    return sqls


def _combine_prior_and_mws(
    prior: float,
    mw_terms: list[str],
    sql_dialect: SplinkDialect,
) -> tuple[str, str]:
    """Compute the combined match weight and match probability expressions (additive).

    This function generates SQL for additive match weight calculation.
    Instead of multiplying Bayes factors, we sum match weights (log2 of BFs).

    Args:
        prior: The prior probability that two random records match
        mw_terms: List of column names containing partial match weights
        sql_dialect: SQL dialect for database-specific functions

    Returns:
        Tuple of (match_weight_expr, match_prob_expr) SQL strings

    Note:
        The prior is clamped to [PROB_CLAMP_MIN, PROB_CLAMP_MAX] to prevent
        numerical issues with log(0) or log(inf).
    """
    # Clamp prior to prevent log(0) and log(inf)
    p = max(PROB_CLAMP_MIN, min(prior, PROB_CLAMP_MAX))
    prior_odds = p / (1 - p)
    mw_prior = math.log2(prior_odds)

    # Build the sum expression: mw_prior + mw_term1 + mw_term2 + ...
    mw_expr = f"cast({mw_prior} as float8) + " + " + ".join(mw_terms)

    # Convert match weight to probability using stable sigmoid
    # match_prob = 1 / (1 + 2^(-match_weight))
    # For numerical stability, use different formulas for positive/negative weights:
    # - When mw >= 0: 1 / (1 + 2^(-mw))
    # - When mw < 0: 2^mw / (1 + 2^mw)
    match_prob_expr = f"""
    CASE
        WHEN ({mw_expr}) >= 0 THEN
            1.0 / (1.0 + POWER(cast(2 as float8), -({mw_expr})))
        ELSE
            POWER(cast(2 as float8), ({mw_expr})) / (1.0 + POWER(cast(2 as float8), ({mw_expr})))
    END
    """.strip()

    return mw_expr, match_prob_expr

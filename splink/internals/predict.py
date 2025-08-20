from __future__ import annotations

# This is otherwise known as the expectation step of the EM algorithm.
import logging
from typing import List

from splink.internals.comparison import Comparison
from splink.internals.dialects import SplinkDialect
from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    prob_to_bayes_factor,
    threshold_args_to_match_weight,
)

from .settings import CoreModelSettings, Settings

logger = logging.getLogger(__name__)


def predict_from_comparison_vectors_sqls_using_settings(
    settings_obj: Settings,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    include_clerical_match_score: bool = False,
    sql_infinity_expression: str = "'infinity'",
) -> list[dict[str, str]]:
    return predict_from_comparison_vectors_sqls(
        unique_id_input_columns=settings_obj.column_info_settings.unique_id_input_columns,
        core_model_settings=settings_obj.core_model_settings,
        sql_dialect=SplinkDialect.from_string(settings_obj._sql_dialect_str),
        threshold_match_probability=threshold_match_probability,
        threshold_match_weight=threshold_match_weight,
        retain_matching_columns=settings_obj._retain_matching_columns,
        retain_intermediate_calculation_columns=settings_obj._retain_intermediate_calculation_columns,
        training_mode=False,
        additional_columns_to_retain=settings_obj._additional_columns_to_retain,
        needs_matchkey_column=settings_obj._needs_matchkey_column,
        include_clerical_match_score=include_clerical_match_score,
        sql_infinity_expression=sql_infinity_expression,
    )


def predict_from_comparison_vectors_sqls(
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
    needs_matchkey_column: bool = False,
    include_clerical_match_score: bool = False,
    sql_infinity_expression: str = "'infinity'",
) -> list[dict[str, str]]:
    sqls = []

    select_cols = Settings.columns_to_select_for_bayes_factor_parts(
        unique_id_input_columns=unique_id_input_columns,
        comparisons=core_model_settings.comparisons,
        retain_matching_columns=retain_matching_columns,
        retain_intermediate_calculation_columns=retain_intermediate_calculation_columns,
        additional_columns_to_retain=additional_columns_to_retain,
        needs_matchkey_column=needs_matchkey_column,
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
        needs_matchkey_column=needs_matchkey_column,
    )
    select_cols_expr = ",".join(select_cols)
    bf_terms = []
    for cc in core_model_settings.comparisons:
        bf_terms.extend(cc._match_weight_columns_to_multiply)

    prior = core_model_settings.probability_two_random_records_match
    bayes_factor_expr, match_prob_expr = _combine_prior_and_bfs(
        prior,
        bf_terms,
        sql_infinity_expression,
        sql_dialect,
    )

    threshold_as_mw = threshold_args_to_match_weight(
        threshold_match_probability, threshold_match_weight
    )

    if threshold_as_mw is not None:
        threshold_expr = f" where log2({bayes_factor_expr}) >= {threshold_as_mw} "
    else:
        threshold_expr = ""

    sql = f"""
    select
    log2({bayes_factor_expr}) as match_weight,
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
    sql_infinity_expression: str = "'infinity'",
) -> list[dict[str, str]]:
    sqls = []

    select_cols = []

    for cc in comparisons:
        cc_sqls = [
            cl._bayes_factor_sql(cc._gamma_column_name) for cl in cc.comparison_levels
        ]
        sql = " ".join(cc_sqls)
        sql = f"CASE {sql} END as {cc._bf_column_name}"
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
        select_cols.append(cc._bf_column_name)
    select_cols.append("agreement_pattern_count")
    select_cols_expr = ",".join(select_cols)

    prior = probability_two_random_records_match
    bf_terms = [cc._bf_column_name for cc in comparisons]
    bayes_factor_expr, match_prob_expr = _combine_prior_and_bfs(
        prior,
        bf_terms,
        sql_infinity_expression,
        sql_dialect,
    )

    sql = f"""
    select
    log2({bayes_factor_expr}) as match_weight,
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


def _combine_prior_and_bfs(
    prior: float,
    bf_terms: list[str],
    sql_infinity_expr: str,
    sql_dialect: SplinkDialect,
) -> tuple[str, str]:
    """Compute the combined Bayes factor and match probability expressions"""
    if prior == 1.0:
        bf_expr = sql_infinity_expr
        match_prob_expr = "1.0"
        return bf_expr, match_prob_expr

    bf_prior = prob_to_bayes_factor(prior)
    bf_expr = f"cast({bf_prior} as float8) * " + " * ".join(bf_terms)

    greatest_name = sql_dialect.greatest_function_name
    least_name = sql_dialect.least_function_name
    bf_expr = f"{least_name}({greatest_name}({bf_expr}, 1e-300), 1e300)"

    mp_raw = f"({bf_expr})/(1+({bf_expr}))"
    # if any BF is Infinity then we need to adjust the match probability
    any_term_inf = " OR ".join((f"{term} = {sql_infinity_expr}" for term in bf_terms))
    match_prob_expr = f"CASE WHEN {any_term_inf} THEN 1.0 ELSE {mp_raw} END"

    return bf_expr, match_prob_expr

from __future__ import annotations

# This is otherwise known as the expectation step of the EM algorithm.
import logging

from .misc import prob_to_bayes_factor, prob_to_match_weight
from .settings import Settings

logger = logging.getLogger(__name__)


def predict_from_comparison_vectors_sqls(
    settings_obj: Settings,
    threshold_match_probability=None,
    threshold_match_weight=None,
    include_clerical_match_score=False,
    sql_infinity_expression="'infinity'",
) -> list[dict]:
    sqls = []

    select_cols = settings_obj._columns_to_select_for_bayes_factor_parts
    select_cols_expr = ",".join(select_cols)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    sql = f"""
    select {select_cols_expr} {clerical_match_score}
    from __splink__df_comparison_vectors
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_match_weight_parts",
    }
    sqls.append(sql)

    select_cols = settings_obj._columns_to_select_for_predict
    select_cols_expr = ",".join(select_cols)
    bf_terms = []
    for cc in settings_obj.comparisons:
        bf_terms.extend(cc._match_weight_columns_to_multiply)

    prior = settings_obj._probability_two_random_records_match
    bayes_factor_expr, match_prob_expr = _combine_prior_and_bfs(
        prior,
        bf_terms,
        sql_infinity_expression,
    )

    # In case user provided both, take the minimum of the two thresholds
    if threshold_match_probability is not None:
        thres_prob_as_weight = prob_to_match_weight(threshold_match_probability)
    else:
        thres_prob_as_weight = None
    if threshold_match_probability or threshold_match_weight:
        thresholds = [
            thres_prob_as_weight,
            threshold_match_weight,
        ]
        threshold = max([t for t in thresholds if t is not None])
        threshold_expr = f" where log2({bayes_factor_expr}) >= {threshold} "
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

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict",
    }
    sqls.append(sql)

    return sqls


def predict_from_agreement_pattern_counts_sqls(
    settings_obj: Settings,
    sql_infinity_expression="'infinity'",
) -> list[dict]:
    sqls = []

    select_cols = []

    for cc in settings_obj.comparisons:
        cc_sqls = [cl._bayes_factor_sql for cl in cc.comparison_levels]
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

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_match_weight_parts",
    }
    sqls.append(sql)

    select_cols = []
    for cc in settings_obj.comparisons:
        select_cols.append(cc._gamma_column_name)
        select_cols.append(cc._bf_column_name)
    select_cols.append("agreement_pattern_count")
    select_cols_expr = ",".join(select_cols)

    prior = settings_obj._probability_two_random_records_match
    bf_terms = [cc._bf_column_name for cc in settings_obj.comparisons]
    bayes_factor_expr, match_prob_expr = _combine_prior_and_bfs(
        prior,
        bf_terms,
        sql_infinity_expression,
    )

    sql = f"""
    select
    log2({bayes_factor_expr}) as match_weight,
    {match_prob_expr} as match_probability,
    {select_cols_expr}
    from __splink__df_match_weight_parts
    """

    sql = {
        "sql": sql,
        "output_table_name": "__splink__df_predict",
    }
    sqls.append(sql)

    return sqls


def _combine_prior_and_bfs(
    prior: float, bf_terms: list[str], sql_infinity_expr: str
) -> tuple[str, str]:
    """Compute the combined Bayes factor and match probability expressions"""
    if prior == 1.0:
        bf_expr = sql_infinity_expr
        match_prob_expr = "1.0"
        return bf_expr, match_prob_expr

    bf_prior = prob_to_bayes_factor(prior)
    bf_expr = f"cast({bf_prior} as float8) * " + " * ".join(bf_terms)

    mp_raw = f"({bf_expr})/(1+({bf_expr}))"
    # if any BF is Infinity then we need to adjust the match probability
    any_term_inf = " OR ".join((f"{term} = {sql_infinity_expr}" for term in bf_terms))
    match_prob_expr = f"CASE WHEN {any_term_inf} THEN 1.0 ELSE {mp_raw} END"

    return bf_expr, match_prob_expr

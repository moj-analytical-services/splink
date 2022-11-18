# This is otherwise known as the expectation step of the EM algorithm.
import logging
from typing import List

from .misc import prob_to_bayes_factor, prob_to_match_weight
from .settings import Settings

logger = logging.getLogger(__name__)


def predict_from_comparison_vectors_sqls(
    settings_obj: Settings,
    threshold_match_probability=None,
    threshold_match_weight=None,
    include_clerical_match_score=False,
    sql_infinity_expression="'infinity'",
) -> List[dict]:

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
    mult = []
    for cc in settings_obj.comparisons:
        mult.extend(cc._match_weight_columns_to_multiply)

    probability_two_random_records_match = (
        settings_obj._probability_two_random_records_match
    )

    if probability_two_random_records_match == 1.0:
        bayes_factor_expr = sql_infinity_expression
        match_prob_expr = "1.0"
    else:
        bayes_factor = prob_to_bayes_factor(probability_two_random_records_match)

        bayes_factor_expr = " * ".join(mult)
        bayes_factor_expr = f"cast({bayes_factor} as double) * {bayes_factor_expr}"

        # if any BF is Infinity then we need to adjust expression,
        # as arithmetic won't go through directly
        any_bf_inf = " OR ".join(
            map(lambda col: f"{col} = {sql_infinity_expression}", mult)
        )
        bayes_factor_expr = (
            f"CASE WHEN {any_bf_inf} THEN {sql_infinity_expression} "
            f"ELSE {bayes_factor_expr} END"
        )
        match_prob_expr = (
            f"CASE WHEN {any_bf_inf} THEN 1.0 "
            f"ELSE (({bayes_factor_expr})/(1+({bayes_factor_expr}))) END"
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

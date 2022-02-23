# This is otherwise known as the expectation step of the EM algorithm.
import logging

from .misc import prob_to_bayes_factor
from .settings import Settings

logger = logging.getLogger(__name__)


def predict(settings_obj: Settings, df_dict, execute_sql):

    # table is called '__splink__df_comparison_vectors'

    select_cols = settings_obj._columns_to_select_for_bayes_factor_parts
    select_cols_expr = ",".join(select_cols)

    sql = f"""
    select {select_cols_expr}
    from __splink__df_comparison_vectors
    """

    df_dict = execute_sql(sql, df_dict, "__splink__df_match_weight_parts")

    select_cols = settings_obj._columns_to_select_for_predict
    select_cols_expr = ",".join(select_cols)
    mult = []
    for cc in settings_obj.comparisons:
        mult.extend(cc.match_weight_columns_to_multiply)

    proportion_of_matches = settings_obj._proportion_of_matches

    bayes_factor = prob_to_bayes_factor(proportion_of_matches)

    bayes_factor_expr = " * ".join(mult)
    bayes_factor_expr = f"{bayes_factor}D * {bayes_factor_expr}"

    sql = f"""
    select
    log2({bayes_factor_expr}) as match_weight,
    (({bayes_factor_expr})/(1+({bayes_factor_expr}))) as match_probability,


    {select_cols_expr}
    from __splink__df_match_weight_parts
    """

    df_dict = execute_sql(sql, df_dict, "__splink__df_predict")

    logger.debug("\n" + sql)

    return df_dict

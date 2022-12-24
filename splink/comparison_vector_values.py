import logging

from .settings import Settings

logger = logging.getLogger(__name__)


def compute_comparison_vector_values_sql(
    settings_obj: Settings, include_clerical_match_score=False
) -> str:
    """Compute the comparison vectors from __splink__df_blocked, the
    dataframe of blocked pairwise record comparisons.

    See [the fastlink paper](https://imai.fas.harvard.edu/research/files/linkage.pdf)
    for more details of what is meant by comparison vectors.
    """

    select_cols = settings_obj._columns_to_select_for_comparison_vector_values

    select_cols_expr = ",".join(select_cols)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    sql = f"""
    select {select_cols_expr} {clerical_match_score}
    from __splink__df_blocked
    """

    return sql

import logging

from .settings import Settings

logger = logging.getLogger(__name__)


def compute_comparison_vector_values_sql(settings_obj: Settings):
    """Compute the comparison vectors and add them to the dataframe.  See
    https://imai.fas.harvard.edu/research/files/linkage.pdf for more details of what is
    meant by comparison vectors

    """

    select_cols = settings_obj._columns_to_select_for_comparison_vector_values

    select_cols_expr = ",".join(select_cols)

    sql = f"""
    select {select_cols_expr}
    from __splink__df_blocked
    """

    return sql

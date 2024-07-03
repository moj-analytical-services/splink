from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Literal, Optional

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)


def compute_comparison_vector_values_sql(
    columns_to_select_for_comparison_vector_values: list[str],
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
    include_clerical_match_score: bool = False,
) -> str:
    """Compute the comparison vectors from __splink__df_blocked, the
    dataframe of blocked pairwise record comparisons.

    See [the fastlink paper](https://imai.fas.harvard.edu/research/files/linkage.pdf)
    for more details of what is meant by comparison vectors.
    """
    select_cols_expr = ",".join(columns_to_select_for_comparison_vector_values)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    if source_dataset_input_column:
        unique_id_columns = [source_dataset_input_column, unique_id_input_column]
    else:
        unique_id_columns = [unique_id_input_column]

    uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
    uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")

    sql = f"""
    select *
    from __splink__df_concat_with_tf as l
    inner join __splink__df_blocked as b
    on {uid_l_expr} = b.join_key_l
    inner join __splink__df_concat_with_tf as r
    on {uid_r_expr} = b.join_key_r
    """

    return sql

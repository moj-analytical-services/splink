from __future__ import annotations

import logging
from typing import List, Optional

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)


def compute_comparison_vector_values_sql(
    columns_to_select_for_comparison_vector_values: list[str],
    include_clerical_match_score: bool = False,
) -> str:
    """Compute the comparison vectors from __splink__df_blocked, the
    dataframe of blocked pairwise record comparisons that includes the various
    columns used for comparisons (`col_l`, `col_r` etc.)

    See [the fastlink paper](https://imai.fas.harvard.edu/research/files/linkage.pdf)
    for more details of what is meant by comparison vectors.
    """
    select_cols_expr = ",".join(columns_to_select_for_comparison_vector_values)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    sql = f"""
    select {select_cols_expr} {clerical_match_score}
    from __splink__df_blocked
    """

    return sql


def compute_comparison_vector_values_from_id_pairs_sqls(
    columns_to_select_for_blocking: List[str],
    columns_to_select_for_comparison_vector_values: list[str],
    input_tablename_l: str,
    input_tablename_r: str,
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
    include_clerical_match_score: bool = False,
) -> list[dict[str, str]]:
    """Compute the comparison vectors from __splink__blocked_id_pairs, the
    materialised dataframe of blocked pairwise record comparisons.

    See [the fastlink paper](https://imai.fas.harvard.edu/research/files/linkage.pdf)
    for more details of what is meant by comparison vectors.
    """
    sqls = []

    if source_dataset_input_column:
        unique_id_columns = [source_dataset_input_column, unique_id_input_column]
    else:
        unique_id_columns = [unique_id_input_column]

    select_cols_expr = ", \n".join(columns_to_select_for_blocking)

    uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
    uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")

    # The first table selects the required columns from the input tables
    # and alises them as `col_l`, `col_r` etc
    # using the __splink__blocked_id_pairs as an associated (junction) table

    # That is, it does the join, but doesn't compute the comparison vectors
    sql = sql = f"""
    select {select_cols_expr}, b.match_key
    from {input_tablename_l} as l
    inner join __splink__blocked_id_pairs as b
    on {uid_l_expr} = b.join_key_l
    inner join {input_tablename_r} as r
    on {uid_r_expr} = b.join_key_r
    """

    sqls.append({"sql": sql, "output_table_name": "blocked_with_cols"})

    select_cols_expr = ", \n".join(columns_to_select_for_comparison_vector_values)

    if include_clerical_match_score:
        clerical_match_score = ", clerical_match_score"
    else:
        clerical_match_score = ""

    # The second table computes the comparison vectors from these aliases
    sql = f"""
    select {select_cols_expr} {clerical_match_score}
    from blocked_with_cols
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__df_comparison_vectors"})

    return sqls

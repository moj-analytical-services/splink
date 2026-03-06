from __future__ import annotations

import logging
from typing import List, Optional

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import (
    _join_condition_nodes_to_blocked_pairs_sql,
)

logger = logging.getLogger(__name__)


def _node_id_tuple_sql(unique_id_columns: list[InputColumn], table_alias: str) -> str:
    cols = [f"{table_alias}.{col.name}" for col in unique_id_columns]
    return f"({', '.join(cols)})"


def _blocked_pair_id_tuple_sql(
    unique_id_columns: list[InputColumn], lr_suffix: str
) -> str:
    cols = [f"{col.unquote().name}{lr_suffix}" for col in unique_id_columns]
    return f"({', '.join(cols)})"


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
    link_type: Optional[str] = None,
    sql_dialect_str: Optional[str] = None,
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

    join_l = _join_condition_nodes_to_blocked_pairs_sql(
        unique_id_columns, "l", "b", "_l"
    )
    join_r = _join_condition_nodes_to_blocked_pairs_sql(
        unique_id_columns, "r", "b", "_r"
    )

    # Where there are large numbers of unmatched records, the DuckDB query planner
    # can struggle with the double inner join below.  It should
    # push the filters down to the input tables, but it doesn't always do this.
    # This forces it.  it is only really relevant in the link only case,
    # where one dataset is much larger than the other
    # This optimisation is here due to poor performance observed in
    # the `uk_address_matcher` package
    # TODO: Once DuckDB 1.5 is released, check this is still needed
    # ref https://github.com/moj-analytical-services/uk_address_matcher/issues/226
    if (
        input_tablename_l == input_tablename_r
        and link_type == "two_dataset_link_only"
        and sql_dialect_str == "duckdb"
    ):
        node_id_tuple = _node_id_tuple_sql(unique_id_columns, "n")
        blocked_id_tuple_l = _blocked_pair_id_tuple_sql(unique_id_columns, "_l")
        blocked_id_tuple_r = _blocked_pair_id_tuple_sql(unique_id_columns, "_r")

        sql = f"""
        select *
        from {input_tablename_l} as n
        where
        {node_id_tuple} in (select {blocked_id_tuple_l} from __splink__blocked_id_pairs)
        or
        {node_id_tuple} in (select {blocked_id_tuple_r} from __splink__blocked_id_pairs)
        """
        sqls.append(
            {"sql": sql, "output_table_name": "__splink__df_concat_with_tf_filtered"}
        )

        input_tablename_l = "__splink__df_concat_with_tf_filtered"
        input_tablename_r = "__splink__df_concat_with_tf_filtered"

    # The first table selects the required columns from the input tables
    # and alises them as `col_l`, `col_r` etc
    # using the __splink__blocked_id_pairs as an associated (junction) table
    # That is, it does the join, but doesn't compute the comparison vectors
    sql = f"""
    select {select_cols_expr}, b.match_key
    from {input_tablename_l} as l
    inner join __splink__blocked_id_pairs as b
    on {join_l}
    inner join {input_tablename_r} as r
    on {join_r}
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

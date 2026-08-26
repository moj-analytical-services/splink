from __future__ import annotations

import logging
import re
from typing import List, Optional

from splink.internals.input_column import InputColumn
from splink.internals.misc import indent_sql
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_nodes_sql,
)

logger = logging.getLogger(__name__)


def _output_alias(select_expression: str) -> str:
    match = re.search(r'\s+AS\s+("[^"]+")\s*$', select_expression, re.IGNORECASE)
    if match is None:
        raise ValueError(f"Could not find output alias in {select_expression!r}")
    return match.group(1)


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
    select_columns = list(columns_to_select_for_comparison_vector_values)
    if include_clerical_match_score:
        select_columns.append("clerical_match_score")

    select_cols_expr = ",\n".join(indent_sql(col) for col in select_columns)

    sql = f"""
    select
{select_cols_expr}
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

    select_columns = [*columns_to_select_for_blocking, "b.match_key"]
    select_cols_expr = ",\n".join(indent_sql(col) for col in select_columns)

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
        uid_expr = _composite_unique_id_from_nodes_sql(unique_id_columns)
        sql = f"""
        select *
        from {input_tablename_l}
        where
        {uid_expr} in (select join_key_l from __splink__blocked_id_pairs)
        or
        {uid_expr} in (select join_key_r from __splink__blocked_id_pairs)
        """

        sqls.append(
            {"sql": sql, "output_table_name": "__splink__df_concat_with_tf_filtered"}
        )
        input_tablename_l = "__splink__df_concat_with_tf_filtered"
        input_tablename_r = "__splink__df_concat_with_tf_filtered"

    uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
    uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")

    # The first table selects the required columns from the input tables
    # and alises them as `col_l`, `col_r` etc
    # using the __splink__blocked_id_pairs as an associated (junction) table
    # That is, it does the join, but doesn't compute the comparison vectors
    sql = f"""
    select
{select_cols_expr}
    from __splink__blocked_id_pairs as b
    inner join {input_tablename_l} as l
    on {uid_l_expr} = b.join_key_l
    inner join {input_tablename_r} as r
    on {uid_r_expr} = b.join_key_r
    """

    sqls.append({"sql": sql, "output_table_name": "blocked_with_cols"})

    select_columns = list(columns_to_select_for_comparison_vector_values)
    if include_clerical_match_score:
        select_columns.append("clerical_match_score")

    select_cols_expr = ",\n".join(indent_sql(col) for col in select_columns)

    # The second table computes the comparison vectors from these aliases
    sql = f"""
    select
{select_cols_expr}
    from blocked_with_cols
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__df_comparison_vectors"})

    return sqls


def compute_comparison_vector_values_from_id_pairs_independent_sqls(
    columns_to_select_for_blocking: List[str],
    columns_to_select_for_comparison_vector_values: list[str],
    input_tablename_l: str,
    input_tablename_r: str,
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
    include_clerical_match_score: bool = False,
) -> list[dict[str, object]]:
    """Hydrate blocked-pair sides independently before recombining them."""
    if source_dataset_input_column:
        unique_id_columns = [source_dataset_input_column, unique_id_input_column]
    else:
        unique_id_columns = [unique_id_input_column]

    left_expressions: list[str] = []
    right_expressions: list[str] = []
    ordered_outputs: list[str] = []
    for expression in columns_to_select_for_blocking:
        stripped = expression.lstrip()
        alias = _output_alias(expression)
        if stripped.startswith('"l".'):
            left_expressions.append(expression)
            ordered_outputs.append(f"hydrated_l.{alias}")
        elif stripped.startswith('"r".'):
            right_expressions.append(expression)
            ordered_outputs.append(f"hydrated_r.{alias}")
        else:
            raise ValueError(
                "Independent hydration requires each blocking payload expression "
                f"to be qualified with l or r: {expression!r}"
            )

    uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
    uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")
    left_select = ",\n".join(indent_sql(expr) for expr in left_expressions)
    right_select = ",\n".join(indent_sql(expr) for expr in right_expressions)

    left_sql = f"""
    select
        b.join_key_l,
        b.join_key_r,
        b.match_key,
{left_select}
    from __splink__blocked_id_pairs as b
    inner join {input_tablename_l} as l
    on {uid_l_expr} = b.join_key_l
    """
    right_sql = f"""
    select
        b.join_key_l,
        b.join_key_r,
        b.match_key,
{right_select}
    from __splink__blocked_id_pairs as b
    inner join {input_tablename_r} as r
    on {uid_r_expr} = b.join_key_r
    """

    blocked_columns = ",\n".join(indent_sql(expr) for expr in ordered_outputs)
    blocked_sql = f"""
    select
{blocked_columns},
        hydrated_l.match_key
    from __splink__left_records as hydrated_l
    inner join __splink__right_records as hydrated_r
    on hydrated_l.join_key_l = hydrated_r.join_key_l
    and hydrated_l.join_key_r = hydrated_r.join_key_r
    and hydrated_l.match_key = hydrated_r.match_key
    """

    comparison_columns = list(columns_to_select_for_comparison_vector_values)
    if include_clerical_match_score:
        comparison_columns.append("clerical_match_score")
    comparison_select = ",\n".join(indent_sql(expr) for expr in comparison_columns)
    comparison_sql = f"""
    select
{comparison_select}
    from __splink__df_blocked
    """

    return [
        {
            "sql": left_sql,
            "output_table_name": "__splink__left_records",
            "materialized": True,
        },
        {
            "sql": right_sql,
            "output_table_name": "__splink__right_records",
            "materialized": True,
        },
        {"sql": blocked_sql, "output_table_name": "__splink__df_blocked"},
        {
            "sql": comparison_sql,
            "output_table_name": "__splink__df_comparison_vectors",
        },
    ]

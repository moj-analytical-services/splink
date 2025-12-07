from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

if TYPE_CHECKING:
    from splink.internals.dialects import SplinkDialect


def _chunk_assignment_sql(
    unique_id_cols: list[InputColumn],
    chunk_num: int,
    num_chunks: int,
    table_prefix: str,
    dialect: "SplinkDialect",
) -> str:
    """
    Generate SQL WHERE clause condition to filter records by chunk.

    Uses deterministic hashing of the composite unique ID.

    Args:
        unique_id_cols: The columns that form the unique ID
        chunk_num: The specific chunk number to filter for (1-indexed)
        num_chunks: Total number of chunks to partition into
        table_prefix: Table alias prefix (e.g., 'l' or 'r')
        dialect: SQL dialect for hash function

    Returns:
        SQL WHERE clause condition like: " AND (ABS(hash(...)) % 3) + 1 = 2",
        or empty string if num_chunks == 1 (no filtering needed)
    """

    if num_chunks == 1:
        return ""

    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix)
    hash_func = dialect.hash_function_name
    chunk_expr = f"(ABS({hash_func}({composite_id})) % {num_chunks}) + 1"
    return f" AND {chunk_expr} = {chunk_num}"

from __future__ import annotations

from typing import TYPE_CHECKING

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
    hash_expr = dialect.hash_function_expression(composite_id)
    chunk_expr = f"(ABS({hash_expr}) % {num_chunks}) + 1"
    return f" AND {chunk_expr} = {chunk_num}"


def _blocked_pairs_cache_key(
    left_chunk: tuple[int, int] | None = None,
    right_chunk: tuple[int, int] | None = None,
) -> str:
    """Generate a cache key for blocked pairs that includes chunk info.

    Args:
        left_chunk: Optional tuple of (chunk_number, total_chunks) for left side.
        right_chunk: Optional tuple of (chunk_number, total_chunks) for right side.

    Returns:
        Cache key string. Examples:
            - No chunking: "__splink__blocked_id_pairs"
            - Chunked: "__splink__blocked_id_pairs_L1of3_R2of4"
    """
    base = "__splink__blocked_id_pairs"

    # No chunking => use base key
    if left_chunk is None and right_chunk is None:
        return base

    # Normalize (1,1) to None for key purposes (no effective chunking)
    if left_chunk == (1, 1):
        left_chunk = None
    if right_chunk == (1, 1):
        right_chunk = None

    if left_chunk is None and right_chunk is None:
        return base

    parts = [base]
    if left_chunk is not None:
        parts.append(f"L{left_chunk[0]}of{left_chunk[1]}")
    if right_chunk is not None:
        parts.append(f"R{right_chunk[0]}of{right_chunk[1]}")

    return "_".join(parts)

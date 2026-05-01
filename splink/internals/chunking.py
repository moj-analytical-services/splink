from __future__ import annotations

from typing import TYPE_CHECKING

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

if TYPE_CHECKING:
    from splink.internals.dialects import SplinkDialect


EM_SAMPLE_BUCKET_COLUMN = "__splink_em_sample_bucket"


def _sql_string_literal(value: str) -> str:
    """Defensively quote a string for safe inclusion as a SQL literal."""
    return "'" + value.replace("'", "''") + "'"


def _em_sampled_input_tablename(input_tablename: str) -> str:
    return f"{input_tablename}_em_sample"


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
        SQL WHERE clause condition like: " AND (hash(... ) % 3) + 1 = 2",
        or empty string if num_chunks == 1 (no filtering needed)
    """

    if num_chunks == 1:
        return ""

    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix)
    chunk_bucket = dialect.hash_bucket_expression(composite_id, num_chunks)
    return f" AND ({chunk_bucket}) + 1 = {chunk_num}"


def _em_sample_table_sql(
    unique_id_cols: list[InputColumn],
    sample_threshold: int,
    sample_modulus: int,
    input_tablename: str,
    dialect: "SplinkDialect",
    salt: str = "__em_sample__",
) -> str:
    """Generate SQL for a sampled input table used upstream of blocking.

    The sample is deterministic and uses a salted hash so that it remains
    statistically independent of the chunking filter produced by
    `_chunk_assignment_sql`.

    Args:
        unique_id_cols: The columns that form the unique ID.
        sample_threshold: Integer in [0, sample_modulus]. A row is retained
            iff hash_bucket(composite_uid || salt, sample_modulus)
            < sample_threshold.
        sample_modulus: Integer giving the resolution of the sampling fraction.
        input_tablename: Input table to sample upstream of blocking.
        dialect: SQL dialect for the hash function.
        salt: String concatenated to the composite uid before hashing.

    Returns:
        SQL statement that materialises the sampled input relation.
    """
    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, "t")
    salted_id = f"({composite_id}) || {_sql_string_literal(salt)}"
    sample_bucket = dialect.hash_bucket_expression(salted_id, sample_modulus)

    if sample_threshold >= sample_modulus:
        return f"select * from {input_tablename}"

    filter_condition = "1=0"
    if sample_threshold > 0:
        filter_condition = f"{EM_SAMPLE_BUCKET_COLUMN} < {sample_threshold}"

    return f"""
    select *
    from (
        select
            *,
            {sample_bucket} as {EM_SAMPLE_BUCKET_COLUMN}
        from {input_tablename} as t
    )
    where {filter_condition}
    """.strip()


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

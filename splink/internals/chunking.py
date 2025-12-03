from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

if TYPE_CHECKING:
    from splink.internals.dialects import SplinkDialect


def _chunk_assignment_sql(
    unique_id_cols: list[InputColumn],
    num_chunks: int,
    table_prefix: str,
    dialect: "SplinkDialect",
) -> str:
    """
    Generate SQL expression that assigns a row to a chunk number (1 to num_chunks).

    Uses deterministic hashing of the composite unique ID.

    Args:
        unique_id_cols: The columns that form the unique ID
        num_chunks: Total number of chunks to partition into
        table_prefix: Table alias prefix (e.g., 'l' or 'r')
        dialect: SQL dialect for hash function

    Returns:
        SQL expression like: "(ABS(hash(...)) % 3) + 1"
    """
    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix)
    hash_func = dialect.hash_function_name
    return f"(ABS({hash_func}({composite_id})) % {num_chunks}) + 1"


def validate_chunk_param(
    chunk: tuple[int, int] | None, param_name: str
) -> tuple[int, int] | None:
    """
    Validate and normalize a chunk parameter.

    Args:
        chunk: Tuple of (chunk_number, total_chunks) or None
        param_name: Name of parameter for error messages

    Returns:
        Validated tuple or None

    Raises:
        ValueError: If chunk parameters are invalid
    """
    if chunk is None:
        return None

    if not isinstance(chunk, tuple) or len(chunk) != 2:
        raise ValueError(
            f"{param_name} must be a tuple of (chunk_number, total_chunks), "
            f"got {chunk}"
        )

    chunk_num, total_chunks = chunk

    if not isinstance(chunk_num, int) or not isinstance(total_chunks, int):
        raise ValueError(
            f"{param_name} must contain integers, "
            f"got ({type(chunk_num).__name__}, {type(total_chunks).__name__})"
        )

    if total_chunks < 1:
        raise ValueError(f"{param_name} total_chunks must be >= 1, got {total_chunks}")

    if chunk_num < 1 or chunk_num > total_chunks:
        raise ValueError(
            f"{param_name} chunk_number must be between 1 and {total_chunks}, "
            f"got {chunk_num}"
        )

    return (chunk_num, total_chunks)


def all_chunk_combinations(
    left_total: int,
    right_total: int,
) -> Iterator[tuple[tuple[int, int], tuple[int, int]]]:
    """
    Generate all (left_chunk, right_chunk) combinations for complete coverage.

    Args:
        left_total: Number of left-side chunks
        right_total: Number of right-side chunks

    Yields:
        Tuples of ((left_n, left_total), (right_n, right_total))

    Example:
        >>> list(all_chunk_combinations(2, 3))
        [((1, 2), (1, 3)), ((1, 2), (2, 3)), ((1, 2), (3, 3)),
         ((2, 2), (1, 3)), ((2, 2), (2, 3)), ((2, 2), (3, 3))]
    """
    for left_n in range(1, left_total + 1):
        for right_n in range(1, right_total + 1):
            yield ((left_n, left_total), (right_n, right_total))

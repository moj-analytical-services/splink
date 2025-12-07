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

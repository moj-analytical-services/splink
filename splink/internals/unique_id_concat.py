from __future__ import annotations

from splink.internals.input_column import InputColumn

CONCAT_SEPARATOR = "-__-"


def _composite_unique_id_from_nodes_sql(
    unique_id_cols: list[InputColumn], table_prefix: str | None = None
) -> str:
    """
    Returns:
        str: e.g. 'l."source_dataset" || -__- || l."unique_id"'
    """
    if table_prefix:
        table_prefix = f"{table_prefix}."
    else:
        table_prefix = ""

    cols = [f"{table_prefix}{c.name}" for c in unique_id_cols]

    return f" || '{CONCAT_SEPARATOR}' || ".join(cols)


def _composite_unique_id_from_edges_sql(unique_id_cols, l_or_r, table_prefix=None):
    """
    Returns:
        str: e.g. '"source_dataset_l" || -__- || "unique_id_l"'
    """

    if table_prefix:
        table_prefix = f"{table_prefix}."
    else:
        table_prefix = ""

    if l_or_r == "l":
        cols = [f"{table_prefix}{c.name_l}" for c in unique_id_cols]
    if l_or_r == "r":
        cols = [f"{table_prefix}{c.name_r}" for c in unique_id_cols]
    if l_or_r is None:
        cols = [f"{table_prefix}{c.name}" for c in unique_id_cols]

    return f" || '{CONCAT_SEPARATOR}' || ".join(cols)


def _individual_uid_columns_as_select_sql(
    unique_id_cols: list[InputColumn],
    table_prefix: str,
    lr_suffix: str,
) -> str:
    """Generate SELECT clause for individual ID columns with _l/_r suffix.

    This creates a SELECT clause that keeps ID columns in their native types
    instead of concatenating them into strings.

    Args:
        unique_id_cols: List of ID columns (e.g., [source_dataset, unique_id])
        table_prefix: Table alias (e.g., "l" or "r")
        lr_suffix: Suffix for output columns (e.g., "_l" or "_r")

    Returns:
        SQL like: l."source_dataset" as source_dataset_l, l."unique_id" as unique_id_l

    Example:
        >>> cols = [InputColumn("source_dataset"), InputColumn("unique_id")]
        >>> _individual_uid_columns_as_select_sql(cols, "l", "_l")
        'l."source_dataset" as source_dataset_l, l."unique_id" as unique_id_l'
    """
    cols = []
    for col in unique_id_cols:
        # Get the unquoted column name for the output alias
        col_unquoted = col.unquote().name
        # Generate the full column reference with table prefix
        quoted_ref = f"{table_prefix}.{col.name}"
        output_name = f"{col_unquoted}{lr_suffix}"
        cols.append(f"{quoted_ref} as {output_name}")
    return ", ".join(cols)


def _join_condition_nodes_to_blocked_pairs_sql(
    unique_id_cols: list[InputColumn],
    node_table_alias: str,
    blocked_pairs_alias: str,
    lr_suffix: str,
) -> str:
    """Generate JOIN condition between nodes table and blocked pairs table.

    Creates a multi-column equality condition for joining the input data
    to the blocked pairs using individual ID columns.

    Args:
        unique_id_cols: List of ID columns
        node_table_alias: Alias for nodes table (e.g., "l")
        blocked_pairs_alias: Alias for blocked pairs table (e.g., "b")
        lr_suffix: Which side to join ("_l" or "_r")

    Returns:
        SQL like: l."source_dataset" = b.source_dataset_l AND l."unique_id" = b.unique_id_l

    Example:
        >>> cols = [InputColumn("source_dataset"), InputColumn("unique_id")]
        >>> _join_condition_nodes_to_blocked_pairs_sql(cols, "l", "b", "_l")
        'l."source_dataset" = b.source_dataset_l AND l."unique_id" = b.unique_id_l'
    """
    conditions = []
    for col in unique_id_cols:
        # Generate node column reference with table prefix
        node_col = f"{node_table_alias}.{col.name}"
        # Generate edge column reference (unquoted name with suffix)
        col_unquoted = col.unquote().name
        edge_col = f"{blocked_pairs_alias}.{col_unquoted}{lr_suffix}"
        conditions.append(f"{node_col} = {edge_col}")
    return " AND ".join(conditions)


def _tuple_comparison_order_condition_sql(
    unique_id_cols: list[InputColumn],
    left_alias: str,
    right_alias: str,
    operator: str = "<",
) -> str:
    """Generate tuple comparison for ordering pairs.

    Creates a multi-column tuple comparison used in WHERE clauses to ensure
    consistent ordering and deduplication of record pairs.

    Args:
        unique_id_cols: List of ID columns
        left_alias: Left table alias (e.g., "l")
        right_alias: Right table alias (e.g., "r")
        operator: Comparison operator (default "<" for ordering)

    Returns:
        SQL like: (l."source_dataset", l."unique_id") < (r."source_dataset", r."unique_id")

    Example:
        >>> cols = [InputColumn("source_dataset"), InputColumn("unique_id")]
        >>> _tuple_comparison_order_condition_sql(cols, "l", "r", "<")
        '(l."source_dataset", l."unique_id") < (r."source_dataset", r."unique_id")'
    """
    left_cols = [f"{left_alias}.{col.name}" for col in unique_id_cols]
    right_cols = [f"{right_alias}.{col.name}" for col in unique_id_cols]

    left_tuple = f"({', '.join(left_cols)})"
    right_tuple = f"({', '.join(right_cols)})"

    return f"{left_tuple} {operator} {right_tuple}"

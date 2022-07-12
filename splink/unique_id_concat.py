CONCAT_SEPARATOR = "-__-"


def _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix=None):
    """
    Returns:
        str: e.g. 'l."source_dataset" || -__- || l."unique_id"'
    """
    if table_prefix:
        table_prefix = f"{table_prefix}."
    else:
        table_prefix = ""

    cols = [f"{table_prefix}{c.name()}" for c in unique_id_cols]

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
        cols = [f"{table_prefix}{c.name_l()}" for c in unique_id_cols]
    if l_or_r == "r":
        cols = [f"{table_prefix}{c.name_r()}" for c in unique_id_cols]
    if l_or_r is None:
        cols = [f"{table_prefix}{c.name()}" for c in unique_id_cols]

    return f" || '{CONCAT_SEPARATOR}' || ".join(cols)

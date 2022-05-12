def _composite_unique_id_from_nodes_sql(unique_id_cols, l_or_r=None):
    """
    Returns:
        str: e.g. 'l."source_dataset" || -__- || l."unique_id"'
    """
    if l_or_r:
        l_or_r = f"{l_or_r}."
    else:
        l_or_r = ""

    cols = [f"{l_or_r}.{c.name()}" for c in unique_id_cols[::-1]]

    return " || '-__-' || ".join(cols)


def _composite_unique_id_from_edges_sql(unique_id_cols, l_or_r):
    """
    Returns:
        str: e.g. '"source_dataset_l" || -__- || "unique_id_l"'
    """

    if l_or_r == "l":
        cols = [c.name_l() for c in unique_id_cols[::-1]]
    if l_or_r == "r":
        cols = [c.name_r() for c in unique_id_cols[::-1]]
    if l_or_r is None:
        cols = [c.name() for c in unique_id_cols[::-1]]

    return " || '-__-' || ".join(cols)

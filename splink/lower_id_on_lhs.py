def _sql_expr_move_left_to_right(
    col_name,
    unique_id_col: str = "unique_id",
    source_dataset_col: str = "source_dataset",
):

    sds_l = f"{source_dataset_col}_l"
    uid_l = f"{unique_id_col}_l"
    sds_r = f"{source_dataset_col}_r"
    uid_r = f"{unique_id_col}_r"
    col_name_l = f"{col_name}_l"
    col_name_r = f"{col_name}_r"

    if source_dataset_col is not None:
        uid_expr_l = f"concat({sds_l}, '-__-', {uid_l})"
        uid_expr_r = f"concat({sds_r}, '-__-', {uid_r})"
    else:
        uid_expr_l = uid_l
        uid_expr_r = uid_r

    move_to_left = f"""
    CASE
    WHEN {uid_expr_l} < {uid_expr_r}
    THEN {col_name_l}
    ELSE {col_name_r}
    END as {col_name_l}
    """

    move_to_right = f"""
    CASE
    WHEN {uid_expr_l} < {uid_expr_r}
    THEN {col_name_r}
    ELSE {col_name_l}
    END as {col_name_r}
    """

    exprs = f"""
    {move_to_left},
    {move_to_right}
    """

    return exprs


def lower_id_to_left_hand_side(
    df,
    source_dataset_col: str = "source_dataset",
    unique_id_col: str = "unique_id",
):
    """Take a dataframe in the format of splink record comparisons (with _l and _r suffixes)
    and return a dataframe where the _l columns correspond to the record with the lower id.
    For example:
    | source_dataset_l   |   unique_id_l | source_dataset_r   |   unique_id_r |   a_l |   a_r | other_col   |
    |:-------------------|--------------:|:-------------------|--------------:|------:|------:|:------------|
    | df                 |             0 | df                 |             1 |     0 |     1 | a           |
    | df                 |             2 | df                 |             0 |     2 |     0 | b           |
    | df                 |             0 | df                 |             3 |     0 |     3 | c           |
    Becomes
    | source_dataset_l   |   unique_id_l | source_dataset_r   |   unique_id_r |   a_l |   a_r | other_col   |
    |:-------------------|--------------:|:-------------------|--------------:|------:|------:|:------------|
    | df                 |             0 | df                 |             1 |     0 |     1 | a           |
    | df                 |             0 | df                 |             2 |     0 |     2 | b           |
    | df                 |             0 | df                 |             3 |     0 |     3 | c           |
    Returns:
        df: a dataframe with the columns _l and _r swapped in the case where
            the unique_id_r < unique_id_l
    """  # noqa

    cols = df.columns
    cols = [c.name(escape=False) for c in cols]

    l_cols = [c for c in cols if c.endswith("_l")]
    r_cols = [c for c in cols if c.endswith("_r")]
    other_cols = [c for c in cols if c not in (l_cols + r_cols)]

    case_exprs = []
    for col in l_cols:
        this_col = col[:-2]
        expr = _sql_expr_move_left_to_right(this_col, unique_id_col, source_dataset_col)
        case_exprs.append(expr)
    case_exprs.extend(other_cols)
    select_expr = ", ".join(case_exprs)

    sql = f"""
    select {select_expr}
    from {df.physical_name}
    """

    return sql

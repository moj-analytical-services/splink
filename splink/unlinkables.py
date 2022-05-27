def unlinkables_data(linker, x_col="match_weight"):
    """Generate data displaying the proportion of records that are "unlinkable"
    for a given splink score threshold and model parameters. These are records that,
    even when compared with themselves, do not contain enough information to confirm
    a match.

    Args:
        linker (Splink): A Splink data linker
        x_col (str, optional): The column name to use as the x-axis in the chart.
            This can be either the "match_weight" or "match_probability" columns.
            Defaults to "match_weight".
        source_dataset (str, optional): Name of the source dataset (used in chart
            title). Defaults to None.
    """

    self_link = linker._self_link()

    if x_col not in ["match_weight", "match_probability"]:
        raise ValueError(
            f"{x_col} must be 'match_weight' (default) or 'match_probability'."
        )

    sql = f"""
        with prob_table as (
            select
            max(round(match_weight, 2)) as match_wei,
            round(match_probability, 5) as match_proba,
            count(*) / cast( sum(count(*)) over () as float) as prop
            from {self_link.physical_name}
            group by match_proba
            order by match_proba
        )
        select *,
        sum(prop) over(order by match_proba) as cum_prop
        from prob_table
        where match_proba < 1
    """

    data = linker._enqueue_and_execute_sql_pipeline(
        sql, "__splink__df_unlinkables", materialise_as_hash=False, use_cache=False
    )

    unlinkables_dict = data.as_record_dict()

    return unlinkables_dict

from .blocking import _sql_gen_where_condition


def analyse_blocking_rule_sql(linker, blocking_rule):

    settings_obj = linker.settings_obj

    link_type = settings_obj._link_type

    if linker.two_dataset_link_only:
        link_type = "two_dataset_link_only"
    where_condition = _sql_gen_where_condition(
        link_type, settings_obj._unique_id_input_columns
    )

    sql = f"""
    select count(*) as count_of_pairwise_comparisons_generated

    from {linker._input_tablename_l} as l
    inner join {linker._input_tablename_r} as r
    on
    {blocking_rule}
    {where_condition}
    """

    return sql

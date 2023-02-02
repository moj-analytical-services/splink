def count_num_comparisons_from_blocking_rules_for_prediction_sql(
    linker,
    df_predict,
):

    if len(linker._settings_obj._blocking_rules_to_generate_predictions) > 1:
        sql = f"""
            sum(match_probability) as est_num_matches, match_key
            from {df_predict.physical_name}
            group by match_key
        """
    else:
        sql = f"""
            sum(match_probability) as est_num_matches
            from {df_predict.physical_name}
        """

    sql = f"""
        select count(*) as count_of_edges,
        {sql}
        order by est_num_matches desc
        """
    return sql

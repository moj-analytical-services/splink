def count_pairwise_comparisons_by_prediction_blocking_rules_sql(df_predict):
    sql = f"""
        select count(*) as count_of_edges,
        sum(match_probability) as est_num_matches, match_key
        from {df_predict.physical_name}
        group by match_key
        order by est_num_matches desc
        """
    return sql

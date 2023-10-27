def _size_density_sql(
    df_predict, df_clustered, threshold_match_probability, _unique_row_id
):
    """Generates sql for computing cluster size and density at a given threshold.

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`
        df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
        threshold_match_probability (float): Filter the pairwise match
            predictions to include only pairwise comparisons with a
            match_probability above this threshold.
        _unique_row_id (string): name of unique id column in settings dict

    Returns:
        sql string for computing cluster size and density
    """

    # Get physical table names from Splink dataframes
    edges_table = df_predict.physical_name
    clusters_table = df_clustered.physical_name

    sqls = []
    sql = f"""
        SELECT
            {_unique_row_id}_l,
            COUNT(*) AS count_edges
        FROM {edges_table}
        WHERE match_probability >= {cluster_threshold}
        GROUP BY {_unique_row_id}_l
    """

    sql = {"sql": sql, "output_table_name": "__count_edges"}
    sqls.append(sql)

    sql = f"""
        SELECT
            c.cluster_id,
            count(*) AS n_nodes,
            sum(e.count_edges) AS n_edges
        FROM {clusters_table} AS c
        LEFT JOIN __count_edges e ON c.{_unique_row_id} = e.{_unique_row_id}_l
        GROUP BY c.cluster_id
    """
    sql = {"sql": sql, "output_table_name": "__counts_per_cluster"}
    sqls.append(sql)

    sql = """
        SELECT
            cluster_id,
            n_nodes,
            n_edges,
            (n_edges * 2)/(n_nodes * (n_nodes-1)) AS density
        FROM __counts_per_cluster
    """
    sql = {"sql": sql, "output_table_name": "__splink__cluster_metrics_clusters"}
    sqls.append(sql)

    return sqls

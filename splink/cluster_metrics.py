from splink.splink_dataframe import SplinkDataFrame


def _node_degree_sql(
    df_predict: SplinkDataFrame,
    composite_uid_edges_l: str,
    composite_uid_edges_r: str,
    threshold_match_probability: float,
):
    sqls = []
    edges_tn = df_predict.physical_name

    sql = f"""
        SELECT
            *
        FROM
            {edges_tn}
        WHERE
            match_probability >= {threshold_match_probability}
    """
    output_tn = "__splink__truncated_edges"
    sql_info = {"sql": sql, "output_table_name": output_tn}
    sqls.append(sql_info)

    sql = f"""
        SELECT
            {composite_uid_edges_l} AS node,
            {composite_uid_edges_r} AS neighbour
        FROM {output_tn}
            UNION ALL
        SELECT
            {composite_uid_edges_r} AS node,
            {composite_uid_edges_l} AS neighbour
        FROM {output_tn}
    """
    output_tn = "__splink__all_nodes"
    sql_info = {"sql": sql, "output_table_name": output_tn}
    sqls.append(sql_info)

    sql = f"""
        SELECT
            node AS composite_unique_id,
            COUNT(*) AS node_degree
        FROM
            {output_tn}
        GROUP BY node
    """
    sql_info = {"sql": sql, "output_table_name": "__splink__node_metrics"}
    sqls.append(sql_info)
    return sqls


def _size_density_sql(
    df_predict: SplinkDataFrame,
    df_clustered: SplinkDataFrame,
    threshold_match_probability: float,
    composite_uid_edges_l: str,
    composite_uid_clusters: str,
):
    """Generates sql for computing cluster size and density at a given threshold.

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`.
        df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`.
        threshold_match_probability (float): Filter the pairwise match
            predictions to include only pairwise comparisons with a
            match_probability above this threshold.
        composite_uid_edges_l (str): unique id for left-hand edges.
        composite_uid_clusters (str): unique id for clusters.


    Returns:
        sql string for computing cluster size and density
    """

    sqls = []
    # Count edges per node at or above a given match probability
    sql = f"""
        SELECT
            {composite_uid_edges_l} AS edge_group_id,
            COUNT(*) AS count_edges
        FROM {df_predict.physical_name}
        WHERE match_probability >= {threshold_match_probability}
        GROUP BY {composite_uid_edges_l}
    """
    sql = {"sql": sql, "output_table_name": "__splink__edges_per_node"}
    sqls.append(sql)

    # Count nodes and edges per cluster
    sql = f"""
        SELECT
            c.cluster_id,
            count(*) AS n_nodes,
            sum(e.count_edges) AS n_edges
        FROM {df_clustered.physical_name} AS c
        LEFT JOIN __splink__edges_per_node e ON
        c.{composite_uid_clusters} = e.edge_group_id
        GROUP BY c.cluster_id
    """
    sql = {"sql": sql, "output_table_name": "__splink__counts_per_cluster"}
    sqls.append(sql)

    # Compute density of each cluster
    sql = """
        SELECT
            cluster_id,
            n_nodes,
            COALESCE(n_edges, 0) AS n_edges,
            (n_edges * 2)/(n_nodes * (n_nodes-1)) AS density
        FROM __splink__counts_per_cluster
    """
    sql = {"sql": sql, "output_table_name": "__splink__cluster_metrics_clusters"}
    sqls.append(sql)

    return sqls

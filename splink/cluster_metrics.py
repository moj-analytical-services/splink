from typing import Dict, List

from splink.splink_dataframe import SplinkDataFrame


def _node_degree_sql(
    df_predict: SplinkDataFrame,
    df_clustered: SplinkDataFrame,
    composite_uid_edges_l: str,
    composite_uid_edges_r: str,
    composite_uid_clusters: str,
    threshold_match_probability: float,
) -> List[Dict[str, str]]:
    """
    Generates sql for computing node degree per node, at a given edge threshold.

    This is includes nodes with no edges, as identified via the clusters table.

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`.
        df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`.
        composite_uid_edges_l (str): unique id for left-hand edges.
        composite_uid_edges_r (str): unique id for right-hand edges.
        composite_uid_clusters (str): unique id for clusters.
        threshold_match_probability (float): Filter the pairwise match
            predictions to include only pairwise comparisons with a
            match_probability at or above this threshold.

    Returns:
        array of dicts, with sql string and output name
        for computing cluster size and density
    """
    sqls = []

    sql = f"""
        SELECT
            *
        FROM {df_predict.physical_name}
        WHERE match_probability >= {threshold_match_probability}
    """
    truncated_edges_table_name = "__splink__truncated_edges"
    sql_info = {"sql": sql, "output_table_name": truncated_edges_table_name}
    sqls.append(sql_info)

    sql = f"""
        SELECT
            {composite_uid_edges_l} AS node,
            {composite_uid_edges_r} AS neighbour
        FROM {truncated_edges_table_name}
            UNION ALL
        SELECT
            {composite_uid_edges_r} AS node,
            {composite_uid_edges_l} AS neighbour
        FROM {truncated_edges_table_name}
    """
    all_nodes_table_name = "__splink__all_nodes"
    sql_info = {"sql": sql, "output_table_name": all_nodes_table_name}
    sqls.append(sql_info)

    # join clusters table to capture edge-less nodes
    # want all clusters included so left join
    sql = f"""
        SELECT
            c.{composite_uid_clusters} AS composite_unique_id,
            c.cluster_id AS cluster_id,
            COUNT(*) FILTER (WHERE neighbour IS NOT NULL) AS node_degree
        FROM
            {df_clustered.physical_name} c
        LEFT JOIN
            {all_nodes_table_name} n
        ON
            c.{composite_uid_clusters} = n.node
        GROUP BY composite_unique_id, cluster_id
    """
    sql_info = {"sql": sql, "output_table_name": "__splink__graph_metrics_nodes"}
    sqls.append(sql_info)
    return sqls


def _size_density_centralisation_sql(
    df_node_metrics: SplinkDataFrame,
) -> List[Dict[str, str]]:
    """
    Generates sql for computing cluster size, density and cluster centralisation.

    The frame df_node_metrics already encodes the relevant information about edges
    and nodes for a given choice of threshold probability.

    Args:
        df_node_metrics (SplinkDataFrame): The results of
            `linker._compute_metrics_nodes()`.

    Returns:
        array of dicts, with sql string and output name
        for computing cluster size and density
    """

    sqls = []
    # Count nodes and edges per cluster
    sql = f"""
        SELECT
            cluster_id,
            COUNT(*) AS n_nodes,
            SUM(node_degree)/2.0 AS n_edges,
            CASE
                WHEN COUNT(*) > 2 THEN
                    1.0*(COUNT(*) * MAX(node_degree) -  SUM(node_degree)) /
                    ((COUNT(*) - 1) * (COUNT(*) - 2))
                ELSE
                    NULL
            END AS cluster_centralisation
        FROM {df_node_metrics.physical_name}
        GROUP BY
            cluster_id
    """
    sql = {"sql": sql, "output_table_name": "__splink__counts_per_cluster"}
    sqls.append(sql)

    # Compute density of each cluster
    sql = """
        SELECT
            cluster_id,
            n_nodes,
            n_edges,
            CASE
                WHEN n_nodes > 1 THEN
                    1.0*(n_edges * 2)/(n_nodes * (n_nodes-1))
                ELSE
                    -- n_nodes is 1 (or 0) density undefined
                    NULL
            END AS density,
            cluster_centralisation
        FROM __splink__counts_per_cluster
    """
    sql = {"sql": sql, "output_table_name": "__splink__graph_metrics_clusters"}
    sqls.append(sql)

    return sqls

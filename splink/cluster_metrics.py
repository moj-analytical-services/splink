from splink.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)


def _size_density_sql(self, df_predict, df_clustered, threshold_match_probability):
    """Generates sql for computing cluster size and density at a given threshold.

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`
        df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
        threshold_match_probability (float): Filter the pairwise match
            predictions to include only pairwise comparisons with a
            match_probability above this threshold.
        _unique_id_col (string): name of unique id column in settings dict

    Returns:
        sql string for computing cluster size and density
    """

    # Get unique id columns from linker
    uid_cols = self._settings_obj._unique_id_input_columns
    # Create unique id for left-hand edges from unique id and source dataset
    composite_uid_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
    # Create unique id for clusters table
    composite_uid_clusters = _composite_unique_id_from_nodes_sql(uid_cols)

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
        LEFT JOIN __splink__edges_per_node e ON c.{composite_uid_clusters} = e.edge_group_id
        GROUP BY c.cluster_id
    """
    sql = {"sql": sql, "output_table_name": "__splink__counts_per_cluster"}
    sqls.append(sql)

    # Compute density of each cluster
    sql = """
        SELECT
            cluster_id,
            n_nodes,
            n_edges,
            (n_edges * 2)/(n_nodes * (n_nodes-1)) AS density
        FROM __splink__counts_per_cluster
    """
    sql = {"sql": sql, "output_table_name": "__splink__cluster_metrics_clusters"}
    sqls.append(sql)

    return sqls

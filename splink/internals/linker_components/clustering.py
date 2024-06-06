from __future__ import annotations

from typing import Optional

from splink.internals.connected_components import (
    _cc_create_unique_id_cols,
    solve_connected_components,
)
from splink.internals.edge_metrics import compute_edge_metrics
from splink.internals.graph_metrics import (
    GraphMetricsResults,
    _node_degree_sql,
    _size_density_centralisation_sql,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)
from splink.internals.vertically_concatenate import (
    compute_df_concat_with_tf,
)


class LinkerClustering:
    def __init__(self, linker):
        self._linker = linker

    def cluster_pairwise_predictions_at_threshold(
        self,
        df_predict: SplinkDataFrame,
        threshold_match_probability: Optional[float] = None,
        pairwise_formatting: bool = False,
        filter_pairwise_format_for_clusters: bool = True,
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from `linker.predict()`
        into groups of connected record using the connected components graph clustering
        algorithm

        Records with an estimated `match_probability` at or above
        `threshold_match_probability` are considered to be a match (i.e. they represent
        the same entity).

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probability (float): Filter the pairwise match predictions
                to include only pairwise comparisons with a match_probability at or
                above this threshold. This dataframe is then fed into the clustering
                algorithm.
            pairwise_formatting (bool): Whether to output the pairwise match predictions
                from linker.predict() with cluster IDs.
                If this is set to false, the output will be a list of all IDs, clustered
                into groups based on the desired match threshold.
            filter_pairwise_format_for_clusters (bool): If pairwise formatting has been
                selected, whether to output all columns found within linker.predict(),
                or just return clusters.

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups based on the desired match threshold.

        """

        # Feeding in df_predict forces materiailisation, if it exists in your database
        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self._linker, pipeline)

        edges_table = _cc_create_unique_id_cols(
            self._linker,
            nodes_with_tf.physical_name,
            df_predict,
            threshold_match_probability,
        )

        cc = solve_connected_components(
            self._linker,
            edges_table,
            df_predict,
            nodes_with_tf,
            pairwise_formatting,
            filter_pairwise_format_for_clusters,
        )
        cc.metadata["threshold_match_probability"] = threshold_match_probability

        return cc

    def _compute_metrics_nodes(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        threshold_match_probability: float,
    ) -> SplinkDataFrame:
        """
        Internal function for computing node-level metrics.

        Accepts outputs of `linker.predict()` and
        `linker.cluster_pairwise_at_threshold()`, along with the clustering threshold
        and produces a table of node metrics.

        Node metrics produced:
        * node_degree (absolute number of neighbouring nodes)

        Output table has a single row per input node, along with the cluster id (as
        assigned in `linker.cluster_pairwise_at_threshold()`) and the metric
        node_degree:
        |-------------------------------------------------|
        | composite_unique_id | cluster_id  | node_degree |
        |---------------------|-------------|-------------|
        | s1-__-10001         | s1-__-10001 | 6           |
        | s1-__-10002         | s1-__-10001 | 4           |
        | s1-__-10003         | s1-__-10003 | 2           |
        ...
        """
        uid_cols = (
            self._linker._settings_obj.column_info_settings.unique_id_input_columns
        )
        # need composite unique ids
        composite_uid_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
        composite_uid_edges_r = _composite_unique_id_from_edges_sql(uid_cols, "r")
        composite_uid_clusters = _composite_unique_id_from_nodes_sql(uid_cols)

        pipeline = CTEPipeline()
        sqls = _node_degree_sql(
            df_predict,
            df_clustered,
            composite_uid_edges_l,
            composite_uid_edges_r,
            composite_uid_clusters,
            threshold_match_probability,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        df_node_metrics = self._linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)

        df_node_metrics.metadata["threshold_match_probability"] = (
            threshold_match_probability
        )
        return df_node_metrics

    def _compute_metrics_edges(
        self,
        df_node_metrics: SplinkDataFrame,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        threshold_match_probability: float,
    ) -> SplinkDataFrame:
        """
        Internal function for computing edge-level metrics.

        Accepts outputs of `linker._compute_node_metrics()`, `linker.predict()` and
        `linker.cluster_pairwise_at_threshold()`, along with the clustering threshold
        and produces a table of edge metrics.

        Uses `igraph` under-the-hood for calculations

        Edge metrics produced:
        * is_bridge (is the edge a bridge?)

        Output table has a single row per edge, and the metric is_bridge:
        |-------------------------------------------------------------|
        | composite_unique_id_l | composite_unique_id_r   | is_bridge |
        |-----------------------|-------------------------|-----------|
        | s1-__-10001           | s1-__-10003             | True      |
        | s1-__-10001           | s1-__-10005             | False     |
        | s1-__-10005           | s1-__-10009             | False     |
        | s1-__-10021           | s1-__-10024             | True      |
        ...
        """
        df_edge_metrics = compute_edge_metrics(
            self, df_node_metrics, df_predict, df_clustered, threshold_match_probability
        )
        df_edge_metrics.metadata["threshold_match_probability"] = (
            threshold_match_probability
        )
        return df_edge_metrics

    def _compute_metrics_clusters(
        self,
        df_node_metrics: SplinkDataFrame,
    ) -> SplinkDataFrame:
        """
        Internal function for computing cluster-level metrics.

        Accepts output of `linker._compute_node_metrics()` (which has the relevant
        information from `linker.predict() and
        `linker.cluster_pairwise_at_threshold()`), produces a table of cluster metrics.

        Cluster metrics produced:
        * n_nodes (aka cluster size, number of nodes in cluster)
        * n_edges (number of edges in cluster)
        * density (number of edges normalised wrt maximum possible number)
        * cluster_centralisation (average absolute deviation from maximum node_degree
            normalised wrt maximum possible value)

        Output table has a single row per cluster, along with the cluster metrics
        listed above
        |--------------------------------------------------------------------|
        | cluster_id  | n_nodes | n_edges | density | cluster_centralisation |
        |-------------|---------|---------|---------|------------------------|
        | s1-__-10006 | 4       | 4       | 0.66667 | 0.6666                 |
        | s1-__-10008 | 6       | 5       | 0.33333 | 0.4                    |
        | s1-__-10013 | 11      | 19      | 0.34545 | 0.3111                 |
        ...
        """
        pipeline = CTEPipeline()
        sqls = _size_density_centralisation_sql(
            df_node_metrics,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        df_cluster_metrics = self._linker.db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )
        df_cluster_metrics.metadata["threshold_match_probability"] = (
            df_node_metrics.metadata["threshold_match_probability"]
        )
        return df_cluster_metrics

    def compute_graph_metrics(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        *,
        threshold_match_probability: float = None,
    ) -> GraphMetricsResults:
        """
        Generates tables containing graph metrics (for nodes, edges and clusters),
        and returns a data class of Splink dataframes

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
            threshold_match_probability (float, optional): Filter the pairwise match
                predictions to include only pairwise comparisons with a
                match_probability at or above this threshold. If not provided, the value
                will be taken from metadata on `df_clustered`. If no such metadata is
                available, this value _must_ be provided.

        Returns:
            GraphMetricsResult: A data class containing SplinkDataFrames
            of cluster IDs and selected node, edge or cluster metrics.
                attribute "nodes" for nodes metrics table
                attribute "edges" for edge metrics table
                attribute "clusters" for cluster metrics table

        """
        if threshold_match_probability is None:
            threshold_match_probability = df_clustered.metadata.get(
                "threshold_match_probability", None
            )
            # we may not have metadata if clusters have been manually registered, or
            # read in from a format that does not include it
            if threshold_match_probability is None:
                raise TypeError(
                    "As `df_clustered` has no threshold metadata associated to it, "
                    "to compute graph metrics you must provide "
                    "`threshold_match_probability` manually"
                )
        df_node_metrics = self._linker._compute_metrics_nodes(
            df_predict, df_clustered, threshold_match_probability
        )
        df_edge_metrics = self._linker._compute_metrics_edges(
            df_node_metrics,
            df_predict,
            df_clustered,
            threshold_match_probability,
        )
        # don't need edges as information is baked into node metrics
        df_cluster_metrics = self._linker._compute_metrics_clusters(df_node_metrics)

        return GraphMetricsResults(
            nodes=df_node_metrics, edges=df_edge_metrics, clusters=df_cluster_metrics
        )

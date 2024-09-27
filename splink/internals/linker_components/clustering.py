from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from splink.internals.connected_components import (
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
from splink.internals.vertically_concatenate import enqueue_df_concat

if TYPE_CHECKING:
    from splink.internals.linker import Linker


class LinkerClustering:
    """Cluster the results of the linkage model and analyse clusters, accessed via
    `linker.clustering`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def cluster_pairwise_predictions_at_threshold(
        self,
        df_predict: SplinkDataFrame,
        threshold_match_probability: Optional[float] = None,
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from
        `linker.inference.predict()` into groups of connected record using the connected
        components graph clustering algorithm

        Records with an estimated `match_probability` at or above
        `threshold_match_probability` are considered to be a match (i.e. they represent
        the same entity).

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probability (float): Pairwise comparisons with a
                `match_probability` at or above this threshold are matched

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups based on the desired match threshold.

        Examples:
            ```python
            df_predict = linker.inference.predict(threshold_match_probability=0.5)
            df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
                df_predict, threshold_match_probability=0.95
            )
            ```
        """

        # Need to get nodes and edges in a format suitable to pass to
        # cluster_pairwise_predictions_at_threshold
        linker = self._linker
        db_api = linker._db_api

        pipeline = CTEPipeline()

        enqueue_df_concat(linker, pipeline)

        uid_cols = linker._settings_obj.column_info_settings.unique_id_input_columns
        uid_concat_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
        uid_concat_edges_r = _composite_unique_id_from_edges_sql(uid_cols, "r")
        uid_concat_nodes = _composite_unique_id_from_nodes_sql(uid_cols, None)

        sql = f"""
        select
            {uid_concat_nodes} as node_id
            from __splink__df_concat
        """
        pipeline.enqueue_sql(sql, "__splink__df_nodes_with_composite_ids")

        nodes_with_composite_ids = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        has_match_prob_col = "match_probability" in [
            c.unquote().name for c in df_predict.columns
        ]

        if not has_match_prob_col and threshold_match_probability is not None:
            raise ValueError(
                "df_predict must have a column called 'match_probability' if "
                "threshold_match_probability is provided"
            )

        match_p_expr = ""
        match_p_select_expr = ""
        if threshold_match_probability is not None:
            match_p_expr = f"where match_probability >= {threshold_match_probability}"
            match_p_select_expr = ", match_probability"

        pipeline = CTEPipeline([df_predict])

        # Templated name must be used here because it could be the output
        # of a deterministic link i.e. the templated name is not know for sure
        sql = f"""
        select
            {uid_concat_edges_l} as node_id_l,
            {uid_concat_edges_r} as node_id_r
            {match_p_select_expr}
            from {df_predict.templated_name}
            {match_p_expr}
        """
        pipeline.enqueue_sql(sql, "__splink__df_edges_from_predict")

        edges_table_with_composite_ids = db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )

        cc = solve_connected_components(
            nodes_table=nodes_with_composite_ids,
            edges_table=edges_table_with_composite_ids,
            node_id_column_name="node_id",
            edge_id_column_name_left="node_id_l",
            edge_id_column_name_right="node_id_r",
            db_api=db_api,
            threshold_match_probability=threshold_match_probability,
        )

        edges_table_with_composite_ids.drop_table_from_database_and_remove_from_cache()
        nodes_with_composite_ids.drop_table_from_database_and_remove_from_cache()
        pipeline = CTEPipeline([cc])

        enqueue_df_concat(linker, pipeline)

        df_obj = next(iter(linker._input_tables_dict.values()))
        columns = df_obj.columns_escaped

        if linker._settings_obj._get_source_dataset_column_name_is_required():
            columns.insert(
                1,
                linker._settings_obj.column_info_settings.source_dataset_input_column.name,
            )

        select_columns_sql = ", ".join(columns)

        sql = f"""
        select
            cc.cluster_id,
            {select_columns_sql}
        from __splink__clustering_output as cc
        left join __splink__df_concat
        on cc.node_id = {uid_concat_nodes}
        """
        pipeline.enqueue_sql(sql, "__splink__df_clustered_with_input_data")

        df_clustered_with_input_data = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        cc.drop_table_from_database_and_remove_from_cache()

        if threshold_match_probability is not None:
            df_clustered_with_input_data.metadata["threshold_match_probability"] = (
                threshold_match_probability
            )

        return df_clustered_with_input_data

    def _compute_metrics_nodes(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        threshold_match_probability: float,
    ) -> SplinkDataFrame:
        """
        Internal function for computing node-level metrics.

        Accepts outputs of `linker.inference.predict()` and
        `linker.clustering.cluster_pairwise_at_threshold()`, along with the clustering
        threshold and produces a table of node metrics.

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

        df_node_metrics = self._linker._db_api.sql_pipeline_to_splink_dataframe(
            pipeline
        )

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

        Accepts outputs of `linker._compute_node_metrics()`,
        `linker.inference.predict()` and
        `linker.clustering.cluster_pairwise_at_threshold()`, along with the clustering
        threshold and produces a table of edge metrics.

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
            self._linker,
            df_node_metrics,
            df_predict,
            df_clustered,
            threshold_match_probability,
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
        `linker.clustering.cluster_pairwise_at_threshold()`), produces a table of
        cluster metrics.

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

        df_cluster_metrics = self._linker._db_api.sql_pipeline_to_splink_dataframe(
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
            df_predict (SplinkDataFrame): The results of `linker.inference.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.clustering.cluster_pairwise_predictions_at_threshold()`
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

        Examples:
            ```python
            df_predict = linker.inference.predict(threshold_match_probability=0.5)
            df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
                df_predict, threshold_match_probability=0.95
            )
            graph_metrics = linker.clustering.compute_graph_metrics(
                df_predict, df_clustered, threshold_match_probability=0.95
            )

            node_metrics = graph_metrics.nodes.as_pandas_dataframe()
            edge_metrics = graph_metrics.edges.as_pandas_dataframe()
            cluster_metrics = graph_metrics.clusters.as_pandas_dataframe()
            ```
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
        df_node_metrics = self._compute_metrics_nodes(
            df_predict, df_clustered, threshold_match_probability
        )
        df_edge_metrics = self._compute_metrics_edges(
            df_node_metrics,
            df_predict,
            df_clustered,
            threshold_match_probability,
        )
        # don't need edges as information is baked into node metrics
        df_cluster_metrics = self._compute_metrics_clusters(df_node_metrics)

        return GraphMetricsResults(
            nodes=df_node_metrics, edges=df_edge_metrics, clusters=df_cluster_metrics
        )

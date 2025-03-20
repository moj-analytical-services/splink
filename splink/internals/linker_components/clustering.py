from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from splink.internals.clustering import (
    _calculate_stable_clusters_at_new_threshold,
    _generate_cluster_summary_stats_sql,
    _generate_detailed_cluster_comparison_sql,
    _get_cluster_stats_sql,
    cluster_pairwise_predictions_at_threshold,
)
from splink.internals.connected_components import (
    solve_connected_components,
)
from splink.internals.edge_metrics import compute_edge_metrics
from splink.internals.graph_metrics import (
    GraphMetricsResults,
    _node_degree_centralisation_sql,
    _size_density_centralisation_sql,
)
from splink.internals.misc import (
    threshold_args_to_match_prob,
    threshold_args_to_match_prob_list,
)
from splink.internals.one_to_one_clustering import (
    one_to_one_clustering,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)
from splink.internals.vertically_concatenate import (
    compute_df_concat,
    concat_table_column_names,
    enqueue_df_concat,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


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
        threshold_match_weight: Optional[float] = None,
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from
        `linker.inference.predict()` into groups of connected record using the connected
        components graph clustering algorithm

        Records with an estimated `match_probability` at or above
        `threshold_match_probability` (or records with a `match_weight` at or above
        `threshold_match_weight`) are considered to be a match (i.e. they represent
        the same entity).

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probability (float, optional): Pairwise comparisons with a
                `match_probability` at or above this threshold are matched
            threshold_match_weight (float, optional): Pairwise comparisons with a
                `match_weight` at or above this threshold are matched. Only one of
                threshold_match_probability or threshold_match_weight should be provided

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

        threshold_match_probability = threshold_args_to_match_prob(
            threshold_match_probability, threshold_match_weight
        )

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

        columns = concat_table_column_names(self._linker)
        # don't want to include salting column in output if present
        columns_without_salt = filter(lambda x: x != "__splink_salt", columns)

        select_columns_sql = ", ".join(columns_without_salt)

        sql = f"""
        select
            cc.cluster_id,
            {select_columns_sql}
        from __splink__clustering_output_final as cc
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

    def cluster_using_single_best_links(
        self,
        df_predict: SplinkDataFrame,
        duplicate_free_datasets: List[str],
        threshold_match_probability: Optional[float] = None,
        threshold_match_weight: Optional[float] = None,
    ) -> SplinkDataFrame:
        """
        Clusters the pairwise match predictions that result from
        `linker.inference.predict()` into groups of connected records using a single
        best links method that restricts the clusters to have at most one record from
        each source dataset in the `duplicate_free_datasets` list.

        This method will include a record into a cluster if it is mutually the best
        match for the record and for the cluster, and if adding the record will not
        violate the criteria of having at most one record from each of the
        `duplicate_free_datasets`.

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            duplicate_free_datasets: (List[str]): The source datasets which should be
                treated as having no duplicates. Clusters will not form with more than
                one record from each of these datasets. This can be a subset of all of
                the source datasets in the input data.
            threshold_match_probability (float, optional): Pairwise comparisons with a
                `match_probability` at or above this threshold are matched
            threshold_match_weight (float, optional): Pairwise comparisons with a
                `match_weight` at or above this threshold are matched. Only one of
                threshold_match_probability or threshold_match_weight should be provided

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups based on the desired match threshold and the source datasets
                for which duplicates are not allowed.

        Examples:
            ```python
            df_predict = linker.inference.predict(threshold_match_probability=0.5)
            df_clustered = linker.clustering.cluster_pairwise_predictions_at_threshold(
                df_predict,
                duplicate_free_datasets=["A", "B"],
                threshold_match_probability=0.95
            )
            ```
        """
        linker = self._linker
        db_api = linker._db_api

        pipeline = CTEPipeline()

        enqueue_df_concat(linker, pipeline)

        uid_cols = linker._settings_obj.column_info_settings.unique_id_input_columns
        uid_concat_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
        uid_concat_edges_r = _composite_unique_id_from_edges_sql(uid_cols, "r")
        uid_concat_nodes = _composite_unique_id_from_nodes_sql(uid_cols, None)

        source_dataset_column_name = (
            linker._settings_obj.column_info_settings.source_dataset_column_name
        )

        sql = f"""
        select
            {uid_concat_nodes} as node_id,
            {source_dataset_column_name} as source_dataset
            from __splink__df_concat
        """
        pipeline.enqueue_sql(sql, "__splink__df_nodes_with_composite_ids")

        nodes_with_composite_ids = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        has_match_prob_col = "match_probability" in [
            c.unquote().name for c in df_predict.columns
        ]

        threshold_match_probability = threshold_args_to_match_prob(
            threshold_match_probability, threshold_match_weight
        )

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

        oo = one_to_one_clustering(
            nodes_table=nodes_with_composite_ids,
            edges_table=edges_table_with_composite_ids,
            node_id_column_name="node_id",
            source_dataset_column_name="source_dataset",
            edge_id_column_name_left="node_id_l",
            edge_id_column_name_right="node_id_r",
            duplicate_free_datasets=duplicate_free_datasets,
            db_api=db_api,
            threshold_match_probability=threshold_match_probability,
        )

        edges_table_with_composite_ids.drop_table_from_database_and_remove_from_cache()
        nodes_with_composite_ids.drop_table_from_database_and_remove_from_cache()
        pipeline = CTEPipeline([oo])

        enqueue_df_concat(linker, pipeline)

        columns = concat_table_column_names(self._linker)
        # don't want to include salting column in output if present
        columns_without_salt = filter(lambda x: x != "__splink_salt", columns)

        select_columns_sql = ", ".join(columns_without_salt)

        sql = f"""
        select
            oo.cluster_id,
            {select_columns_sql}
        from {oo.templated_name} as oo
        left join __splink__df_concat
        on oo.node_id = {uid_concat_nodes}
        """
        pipeline.enqueue_sql(sql, "__splink__df_clustered_with_input_data")

        df_clustered_with_input_data = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        oo.drop_table_from_database_and_remove_from_cache()

        if threshold_match_probability is not None:
            df_clustered_with_input_data.metadata["threshold_match_probability"] = (
                threshold_match_probability
            )

        return df_clustered_with_input_data

    def cluster_pairwise_predictions_at_multiple_thresholds(
        self,
        df_predict: SplinkDataFrame,
        threshold_match_probabilities: Optional[list[float]] | None = None,
        threshold_match_weights: Optional[list[float]] | None = None,
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from
        `linker.inference.predict()` into groups of connected record using the connected
        components graph clustering algorithm

        Records with an estimated `match_probability` at or above each of the values in
        `threshold_match_probabilities` (or records with a `match_weight` at or above
        each of the values in `threshold_match_weights`) are considered to be a match
        (i.e. they represent the same entity).

        This function efficiently computes clusters for multiple thresholds by starting
        with the lowest threshold and incrementally updating clusters for higher
         thresholds.

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probabilities (list[float] | None): List of match
                probability thresholds to compute clusters for
            threshold_match_weights (list[float] | None): List of match weight
                 thresholds to compute clusters for

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups for each of the desired match thresholds.

                The output dataframe will contain the following metadata:

                - threshold_match_probabilities: List of match probability thresholds

                - cluster_summary_stats: summary statistics (number of clusters, max
                 cluster size, avg cluster size) for each threshold

        Examples:
            ```python
            df_predict = linker.inference.predict(threshold_match_probability=0.5)
            df_clustered =
                linker.clustering.cluster_pairwise_predictions_at_multiple_thresholds(
                df_predict, threshold_match_probability=0.95
            )

            # Access the match probability thresholds
            match_prob_thresholds = df_clustered
                .metadata["threshold_match_probabilities"]

            # Access the cluster summary stats
            cluster_summary_stats = df_clustered.metadata["cluster_summary_stats"]
            ```

        """

        # Strategy to cluster at multiple thresholds:
        # 1. Cluster at the lowest threshold
        # 2. At next threshold, note that some clusters do not need to be recomputed.
        #    Specifically, those where the min probability within the cluster is
        #    greater than this next threshold.
        # 3. Compute remaining clusters which _are_ affected by the next threshold.
        # 4. Repeat for remaining thresholds.

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

        # Input could either be user data, or a SplinkDataFrame
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

        is_match_weight = (
            threshold_match_weights is not None
            and threshold_match_probabilities is None
        )

        threshold_match_probabilities = threshold_args_to_match_prob_list(
            threshold_match_probabilities, threshold_match_weights
        )

        if (
            threshold_match_probabilities is None
            or len(threshold_match_probabilities) == 0
        ):
            raise ValueError(
                "Must provide either threshold_match_probabilities "
                "or threshold_match_weights"
            )

        if not has_match_prob_col and threshold_match_probabilities is not None:
            raise ValueError(
                "df_predict must have a column called 'match_probability' if "
                "threshold_match_probability is provided"
            )

        initial_threshold = threshold_match_probabilities.pop(0)
        all_results = {}
        all_results_summary = {}

        match_p_expr = ""
        match_p_select_expr = ""
        if initial_threshold is not None:
            match_p_expr = f"where match_probability >= {initial_threshold}"
            match_p_select_expr = ", match_probability"

        pipeline = CTEPipeline([df_predict])

        # Templated name must be used here because it could be the output
        # of a deterministic link i.e. the physical name is not know for sure
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
        logger.info(f"--------Clustering at threshold {initial_threshold}--------")
        # First cluster at the lowest threshold
        cc = cluster_pairwise_predictions_at_threshold(
            nodes=nodes_with_composite_ids,
            edges=edges_table_with_composite_ids,
            db_api=db_api,
            node_id_column_name="node_id",
            edge_id_column_name_left="node_id_l",
            edge_id_column_name_right="node_id_r",
            threshold_match_probability=initial_threshold,
        )

        all_results[initial_threshold] = cc

        # Calculate Summary stats for first clustering threshold
        pipeline = CTEPipeline([cc])
        sqls = _get_cluster_stats_sql(cc)
        pipeline.enqueue_list_of_sqls(sqls)
        cc_summary = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        all_results_summary[initial_threshold] = cc_summary

        # Now iterate over the remaining thresholds
        previous_threshold = initial_threshold
        for new_threshold in threshold_match_probabilities:
            # Get stable nodes
            logger.info(f"--------Clustering at threshold {new_threshold}--------")
            pipeline = CTEPipeline([cc, edges_table_with_composite_ids])

            sqls = _calculate_stable_clusters_at_new_threshold(
                edges_sdf=edges_table_with_composite_ids,
                cc=cc,
                node_id_column_name="node_id",
                edge_id_column_name_left="node_id_l",
                edge_id_column_name_right="node_id_r",
                previous_threshold_match_probability=previous_threshold,
                new_threshold_match_probability=new_threshold,
            )

            pipeline.enqueue_list_of_sqls(sqls)
            stable_clusters = db_api.sql_pipeline_to_splink_dataframe(pipeline)

            # Derive nodes in play and edges in play by removing stable nodes.  Then
            # run cluster_pairwise_predictions_at_threshold at new threhold

            pipeline = CTEPipeline([nodes_with_composite_ids, stable_clusters])
            sql = f"""
            select *
            from {nodes_with_composite_ids.templated_name}
            where node_id not in
            (select node_id from {stable_clusters.templated_name})
            """
            pipeline.enqueue_sql(sql, "__splink__nodes_in_play")
            nodes_in_play = db_api.sql_pipeline_to_splink_dataframe(pipeline)

            pipeline = CTEPipeline([nodes_in_play, edges_table_with_composite_ids])
            sql = f"""
            select *
            from {edges_table_with_composite_ids.templated_name}
            where node_id_l in
            (select node_id from {nodes_in_play.templated_name})
            and node_id_r in
            (select node_id from {nodes_in_play.templated_name})
            """
            pipeline.enqueue_sql(sql, "__splink__edges_in_play")
            edges_in_play = db_api.sql_pipeline_to_splink_dataframe(pipeline)

            marginal_new_clusters = cluster_pairwise_predictions_at_threshold(
                nodes_in_play,
                edges_in_play,
                node_id_column_name="node_id",
                edge_id_column_name_left="node_id_l",
                edge_id_column_name_right="node_id_r",
                db_api=db_api,
                threshold_match_probability=new_threshold,
            )

            pipeline = CTEPipeline([stable_clusters, marginal_new_clusters])
            sql = f"""
            SELECT node_id, cluster_id
            FROM {stable_clusters.templated_name}
            UNION ALL
            SELECT node_id, cluster_id
            FROM {marginal_new_clusters.templated_name}
            """

            pipeline.enqueue_sql(sql, "__splink__clusters_at_threshold")

            cc = db_api.sql_pipeline_to_splink_dataframe(pipeline)

            previous_threshold = new_threshold

            edges_in_play.drop_table_from_database_and_remove_from_cache()
            nodes_in_play.drop_table_from_database_and_remove_from_cache()
            stable_clusters.drop_table_from_database_and_remove_from_cache()
            marginal_new_clusters.drop_table_from_database_and_remove_from_cache()

            all_results[new_threshold] = cc

            # Calculate summary stats for metadata
            pipeline = CTEPipeline([cc])
            sqls = _get_cluster_stats_sql(cc)
            pipeline.enqueue_list_of_sqls(sqls)
            cc_summary = db_api.sql_pipeline_to_splink_dataframe(pipeline)
            all_results_summary[new_threshold] = cc_summary

        sql = _generate_detailed_cluster_comparison_sql(
            all_results,
            unique_id_col="node_id",
            is_match_weight=is_match_weight,
        )
        pipeline = CTEPipeline()
        pipeline.enqueue_sql(sql, "__splink__clusters_at_all_thresholds")
        joined = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline()
        concat = compute_df_concat(linker, pipeline)

        columns = concat_table_column_names(self._linker)
        # don't want to include salting column in output if present
        columns_without_salt = filter(lambda x: x != "__splink_salt", columns)

        select_columns_sql = ", ".join(columns_without_salt)

        pipeline = CTEPipeline([joined, concat])
        sql = f"""
        select
            co.*,
            {select_columns_sql}
        from {joined.physical_name} as co
        left join {concat.physical_name} as c
        on co.node_id = {uid_concat_nodes}
        """
        pipeline.enqueue_sql(
            sql, "__splink__clusters_at_all_thresholds_with_input_data"
        )

        df_clustered_with_input_data = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        # Add metadata to the output dataframe
        ## Match probability thresholds
        df_clustered_with_input_data.metadata["threshold_match_probabilities"] = [
            initial_threshold
        ] + threshold_match_probabilities

        ## Cluster Summary stats
        pipeline = CTEPipeline()
        sql = _generate_cluster_summary_stats_sql(all_results_summary)
        pipeline.enqueue_sql(sql, "__splink__cluster_summary_stats")
        df_clustered_with_input_data.metadata["cluster_summary_stats"] = (
            db_api.sql_pipeline_to_splink_dataframe(pipeline)
        )

        # Drop cached tables
        for v in all_results.values():
            v.drop_table_from_database_and_remove_from_cache()
        cc.drop_table_from_database_and_remove_from_cache()

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
        * node_centralisation (proportion of neighbours wrt maximum possible number)

        Output table has a single row per input node, along with the cluster id (as
        assigned in `linker.cluster_pairwise_at_threshold()`) and the metrics
        node_degree and node_centralisation:

        |-----------------------------------------------------------------------|
        | composite_unique_id | cluster_id  | node_degree | node_centralisation |
        |---------------------|-------------|-------------|---------------------|
        | s1-__-10001         | s1-__-10001 | 6           | 0.9                 |
        | s1-__-10002         | s1-__-10001 | 4           | 0.6                 |
        | s1-__-10003         | s1-__-10003 | 2           | 0.3                 |
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
        sqls = _node_degree_centralisation_sql(
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

                - attribute "nodes" for nodes metrics table

                - attribute "edges" for edge metrics table

                - attribute "clusters" for cluster metrics table

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

            # Access the match probability thresholds
            probability_threshold = graph_metrics.nodes.metadata
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

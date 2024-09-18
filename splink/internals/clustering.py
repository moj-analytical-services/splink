import time
from typing import List, Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame


def cluster_pairwise_predictions_at_threshold(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
    threshold_match_probability: Optional[float] = None,
) -> SplinkDataFrame:
    if not isinstance(nodes, SplinkDataFrame):
        nodes_sdf = db_api.register_table(nodes, "__splink__df_nodes", overwrite=True)
    else:
        nodes_sdf = nodes

    if not isinstance(edges, SplinkDataFrame):
        edges_sdf = db_api.register_table(edges, "__splink__df_edges", overwrite=True)
    else:
        edges_sdf = edges

    if not edge_id_column_name_left:
        edge_id_column_name_left = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_l

    if not edge_id_column_name_right:
        edge_id_column_name_right = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_r

    cc = solve_connected_components(
        nodes_table=nodes_sdf,
        edges_table=edges_sdf,
        node_id_column_name=node_id_column_name,
        edge_id_column_name_left=edge_id_column_name_left,
        edge_id_column_name_right=edge_id_column_name_right,
        db_api=db_api,
        threshold_match_probability=threshold_match_probability,
    )
    cc.metadata["threshold_match_probability"] = threshold_match_probability
    return cc


def cluster_pairwise_predictions_at_multiple_thresholds(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    match_probability_thresholds: List[float],
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
) -> SplinkDataFrame:
    tid = ascii_uid(8)
    if not isinstance(nodes, SplinkDataFrame):
        nodes_sdf = db_api.register_table(
            nodes, f"__splink__df_nodes_{tid}", overwrite=True
        )
    else:
        nodes_sdf = nodes

    if not isinstance(edges, SplinkDataFrame):
        edges_sdf = db_api.register_table(
            edges, f"__splink__df_edges_{tid}", overwrite=True
        )
    else:
        edges_sdf = edges

    if not edge_id_column_name_left:
        edge_id_column_name_left = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_l

    if not edge_id_column_name_right:
        edge_id_column_name_right = InputColumn(
            node_id_column_name,
            sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect,
        ).name_r

    match_probability_thresholds = sorted(match_probability_thresholds)

    INITIAL_THRESHOLD = match_probability_thresholds.pop(0)
    all_results = {}

    # First cluster at the lowest threshold
    cc = cluster_pairwise_predictions_at_threshold(
        nodes=nodes,
        edges=edges,
        db_api=db_api,
        node_id_column_name=node_id_column_name,
        edge_id_column_name_left=edge_id_column_name_left,
        edge_id_column_name_right=edge_id_column_name_right,
        threshold_match_probability=INITIAL_THRESHOLD,
    )
    all_results[INITIAL_THRESHOLD] = cc

    # print(f"Original clustering results at threshold {INITIAL_THRESHOLD}")
    # print(cc.as_duckdbpyrelation())
    start_time_pre_query = time.time()
    pipeline = CTEPipeline([cc, edges_sdf])

    sql = f"""
    SELECT * from {edges_sdf.templated_name}
    WHERE match_probability >= {INITIAL_THRESHOLD}
    """
    pipeline.enqueue_sql(sql, "__splink__relevant_edges")

    # Calculate cluster_min_probs
    sql = f"""
    SELECT
        c.cluster_id,
        e.match_probability
    FROM {cc.physical_name} c
    LEFT JOIN __splink__relevant_edges e
    ON c.{node_id_column_name} = e.{edge_id_column_name_left}
    UNION ALL
    SELECT
        c.cluster_id,
        e.match_probability
    FROM {cc.physical_name} c
    LEFT JOIN __splink__relevant_edges e
    ON c.{node_id_column_name} = e.{edge_id_column_name_right}
    """
    pipeline.enqueue_sql(sql, "__splink__cluster_edge_probabilities")

    # Step 2: Find the min probability
    sql = """
    SELECT
        cluster_id,
        COALESCE(MIN(match_probability), 1.0) AS min_match_probability
    FROM __splink__cluster_edge_probabilities
    GROUP BY cluster_id
    """
    pipeline.enqueue_sql(sql, "__splink__cluster_min_probs")

    # Join with nodes to get one row per node

    sql = f"""
    SELECT
        n.cluster_id,
        n.{node_id_column_name},

        cmp.min_match_probability
    FROM {cc.physical_name} n
    JOIN __splink__cluster_min_probs cmp ON n.cluster_id = cmp.cluster_id
    """
    pipeline.enqueue_sql(sql, "__splink__node_cluster_min_probabilities")

    node_cluster_min_probabilities = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    # node_cluster_min_probabilities is
    # |   cluster_id |   node_id |   min_match_probability |
    # |-------------:|----------:|------------------------:|
    # |            1 |         1 |                     0.8 |
    # |            1 |         2 |                     0.8 |
    # |            1 |         3 |                     0.8 |
    # |            4 |         4 |                     1   |

    # Next is the main loop where we calculate the result for the rest of the thresholds
    OLD_THRESHOLD = INITIAL_THRESHOLD

    # print("node_cluster_min_probabilities")
    # print(node_cluster_min_probabilities.as_duckdbpyrelation())

    # print("----------------------------------")
    end_time_pre_query = time.time()
    print(
        f"Time taken to pre-query: {end_time_pre_query - start_time_pre_query:.2f} seconds"
    )
    for NEW_THRESHOLD in match_probability_thresholds:
        print(f"Now clustering at threshold {NEW_THRESHOLD}")
        start_time = time.time()
        # First filter the edges and the nodes to remove any where
        # min_match_probability is > the NEW_THRESHOLD

        pipeline = CTEPipeline([node_cluster_min_probabilities])
        sql = f"""
        SELECT
            cluster_id,
            {node_id_column_name},
            min_match_probability
        FROM __splink__node_cluster_min_probabilities
        WHERE min_match_probability >= {NEW_THRESHOLD}
        """
        pipeline.enqueue_sql(sql, "__splink__stable_nodes_from_initial_threshold")

        if OLD_THRESHOLD != INITIAL_THRESHOLD:  # if not t
            # For each clusters in the new clusters, get the new min_match_probability
            # Step 1: Split the LEFT JOIN across a UNION ALL
            sql = f"""
            SELECT
                c.cluster_id,
                e.match_probability
            FROM {marginal_new_clusters.physical_name} c
            LEFT JOIN {edges_in_play.physical_name} e
            ON c.{node_id_column_name} = e.{edge_id_column_name_left}
            UNION ALL
            SELECT
                c.cluster_id,
                e.match_probability
            FROM {marginal_new_clusters.physical_name} c
            LEFT JOIN {edges_in_play.physical_name} e
            ON c.{node_id_column_name} = e.{edge_id_column_name_right}
            """
            pipeline.enqueue_sql(sql, "__splink__additional_cluster_edge_probabilities")

            # Step 2: Find the min probability
            sql = """
            SELECT
                cluster_id,
                COALESCE(MIN(match_probability), 1.0) AS min_match_probability
            FROM __splink__additional_cluster_edge_probabilities
            GROUP BY cluster_id
            """
            pipeline.enqueue_sql(sql, "__splink__additional_cluster_min_probs")

            # But what we need is the node ids corresponding to these clusters
            sql = f"""
            SELECT
                n.cluster_id,
                n.{node_id_column_name},
                cmp.min_match_probability
            FROM {marginal_new_clusters.physical_name} n
            JOIN __splink__additional_cluster_min_probs cmp ON n.cluster_id = cmp.cluster_id
            WHERE cmp.min_match_probability >= {NEW_THRESHOLD}
            """
            pipeline.enqueue_sql(
                sql, "__splink__additional_node_cluster_min_probabilities"
            )

            sql = """
            select * from __splink__additional_node_cluster_min_probabilities
            UNION ALL
            select * from __splink__stable_nodes_from_initial_threshold
            """
            pipeline.enqueue_sql(sql, "__splink__stable_nodes")
        else:
            sql = """
            select * from __splink__stable_nodes_from_initial_threshold
            """
            pipeline.enqueue_sql(sql, "__splink__stable_nodes")

        stable_nodes = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )
        # print("stable_nodes")
        # print(stable_nodes.as_duckdbpyrelation())

        # Filter the nodes to remove any where min_match_probability is < the NEW_THRESHOLD

        pipeline = CTEPipeline([nodes_sdf, stable_nodes])
        sql = f"""
        SELECT
            {node_id_column_name}
        FROM {nodes_sdf.templated_name}
        WHERE {node_id_column_name} not in (SELECT {node_id_column_name} FROM __splink__stable_nodes)
        """
        pipeline.enqueue_sql(sql, "__splink__nodes_in_play")
        nodes_in_play = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )
        print("nodes_in_play")
        print(nodes_in_play.as_duckdbpyrelation().count("*"))

        # Filter edges to keep only those not in stable clusters
        pipeline = CTEPipeline([edges_sdf, stable_nodes])
        sql = f"""
        SELECT
            e.{edge_id_column_name_left},
            e.{edge_id_column_name_right},
            e.match_probability
        FROM {edges_sdf.templated_name} e
        WHERE e.{edge_id_column_name_left} NOT IN (SELECT {node_id_column_name} FROM __splink__stable_nodes)
          AND e.{edge_id_column_name_right} NOT IN (SELECT {node_id_column_name} FROM __splink__stable_nodes)
          AND e.match_probability >= {OLD_THRESHOLD}
        """

        pipeline.enqueue_sql(sql, "__splink__edges_in_play")

        edges_in_play = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )
        print("edges_in_play")
        # print(edges_in_play.as_duckdbpyrelation())
        print(edges_in_play.as_duckdbpyrelation().count("*"))

        # Now we cluster the ones still in play
        start_time_clustering = time.time()
        marginal_new_clusters = cluster_pairwise_predictions_at_threshold(
            nodes=nodes_in_play,
            edges=edges_in_play,
            db_api=db_api,
            node_id_column_name=node_id_column_name,
            edge_id_column_name_left=edge_id_column_name_left,
            edge_id_column_name_right=edge_id_column_name_right,
            threshold_match_probability=NEW_THRESHOLD,
        )
        end_time_clustering = time.time()
        print(
            f"Time taken to cluster at threshold {NEW_THRESHOLD}: {end_time_clustering - start_time_clustering:.2f} seconds"
        )

        pipeline = CTEPipeline([stable_nodes, marginal_new_clusters])
        sql = f"""
        SELECT cluster_id, {node_id_column_name} FROM {stable_nodes.templated_name}
        UNION ALL
        SELECT cluster_id, {node_id_column_name} FROM {marginal_new_clusters.templated_name}
        """

        pipeline.enqueue_sql(sql, "__splink__all_clusters_unsorted")

        sql = f"""
        SELECT * FROM __splink__all_clusters_unsorted
        ORDER BY cluster_id, {node_id_column_name}
        """
        pipeline.enqueue_sql(sql, "__splink__all_clusters")
        final_new_clusters = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        all_results[NEW_THRESHOLD] = final_new_clusters
        OLD_THRESHOLD = NEW_THRESHOLD
        end_time = time.time()
        print(f"Iteration Time taken: {end_time - start_time:.2f} seconds")

        # print("final_new_clusters")
        # print(final_new_clusters.as_duckdbpyrelation())
        # print("----------------------------------")

    return all_results

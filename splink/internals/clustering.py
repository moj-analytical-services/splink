from __future__ import annotations

from typing import Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame


def _get_edge_id_column_names(
    node_id_column_name: str,
    edge_id_column_name_left: str,
    edge_id_column_name_right: str,
    db_api: DatabaseAPISubClass,
) -> tuple[str, str]:
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

    return edge_id_column_name_left, edge_id_column_name_right


def cluster_pairwise_predictions_at_threshold(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
    threshold_match_probability: Optional[float] = None,
) -> SplinkDataFrame:
    """Clusters the pairwise match predictions into groups of connected records using
    the connected components graph clustering algorithm.

    Records with an estimated match probability at or above threshold_match_probability
    are considered to be a match (i.e. they represent the same entity).

    If no match probability column is provided, it is assumed that all edges
    (comparison) are a match.

    If your node and edge column names follow Splink naming conventions, then you can
    omit edge_id_column_name_left and edge_id_column_name_right. For example, if you
    have a table of nodes with a column `unique_id`, it would be assumed that the
    edge table has columns `unique_id_l` and `unique_id_r`.

    Args:
        nodes (AcceptableInputTableType): The table containing node information
        edges (AcceptableInputTableType): The table containing edge information
        db_api (DatabaseAPISubClass): The database API to use for querying
        node_id_column_name (str): The name of the column containing node IDs
        edge_id_column_name_left (Optional[str]): The name of the column containing
            left edge IDs. If not provided, assumed to be f"{node_id_column_name}_l"
        edge_id_column_name_right (Optional[str]): The name of the column containing
            right edge IDs. If not provided, assumed to be f"{node_id_column_name}_r"
        threshold_match_probability (Optional[float]): Pairwise comparisons with a
            match_probability at or above this threshold are matched

    Returns:
        SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
            into groups based on the desired match threshold.

    Examples:
        ```python
        from splink import DuckDBAPI
        from splink.clustering import cluster_pairwise_predictions_at_threshold

        db_api = DuckDBAPI()

        nodes = [
            {"my_id": 1},
            {"my_id": 2},
            {"my_id": 3},
            {"my_id": 4},
            {"my_id": 5},
            {"my_id": 6},
        ]

        edges = [
            {"n_1": 1, "n_2": 2, "match_probability": 0.8},
            {"n_1": 3, "n_2": 2, "match_probability": 0.9},
            {"n_1": 4, "n_2": 5, "match_probability": 0.99},
        ]

        cc = cluster_pairwise_predictions_at_threshold(
            nodes,
            edges,
            node_id_column_name="my_id",
            edge_id_column_name_left="n_1",
            edge_id_column_name_right="n_2",
            db_api=db_api,
            threshold_match_probability=0.5,
        )

        cc.as_duckdbpyrelation()
        ```
    """

    uid = ascii_uid(8)

    if isinstance(nodes, SplinkDataFrame):
        nodes_sdf = nodes
    else:
        nodes_sdf = db_api.register_table(nodes, f"__splink__df_nodes_{uid}")

    if isinstance(edges, SplinkDataFrame):
        edges_sdf = edges
    else:
        edges_sdf = db_api.register_table(edges, f"__splink__df_edges_{uid}")

    edge_id_column_name_left, edge_id_column_name_right = _get_edge_id_column_names(
        node_id_column_name,
        edge_id_column_name_left,
        edge_id_column_name_right,
        db_api,
    )

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


def _calculate_stable_clusters_at_new_threshold(
    edges_sdf: SplinkDataFrame,
    cc: SplinkDataFrame,
    node_id_column_name: str,
    edge_id_column_name_left: str,
    edge_id_column_name_right: str,
    previous_threshold_match_probability: float,
    new_threshold_match_probability: float,
) -> list[dict[str, str]]:
    """Generate SQL to calculate minimum match probabilities for each cluster.

    Args:
        edges_sdf (SplinkDataFrame): DataFrame containing edge information.
        cc (SplinkDataFrame): DataFrame containing cluster information.
        node_id_column_name (str): Name of the column containing node IDs.
        edge_id_column_name_left (str): Name of the left edge ID column.
        edge_id_column_name_right (str): Name of the right edge ID column.
        initial_threshold (float): Initial threshold for match probability.

    Returns:
        List[dict]: List of SQL queries and their output table names.
    """
    sqls = []

    # Filter relevant edges
    sql = f"""
    SELECT * from {edges_sdf.templated_name}
    WHERE match_probability >= {previous_threshold_match_probability}
    """
    sqls.append({"sql": sql, "output_table_name": "__splink__relevant_edges"})

    # Calculate cluster edge probabilities
    sql = f"""
    SELECT
        c.cluster_id,

        e.match_probability
    FROM {cc.templated_name} c
    LEFT JOIN __splink__relevant_edges e
    ON c.{node_id_column_name} = e.{edge_id_column_name_left}

    UNION ALL

    SELECT
        c.cluster_id,
        e.match_probability
    FROM {cc.templated_name} c
    LEFT JOIN __splink__relevant_edges e
    ON c.{node_id_column_name} = e.{edge_id_column_name_right}

    """
    sqls.append(
        {"sql": sql, "output_table_name": "__splink__cluster_edge_probabilities"}
    )

    sql = f"""
    SELECT

        cluster_id

    FROM __splink__cluster_edge_probabilities
    GROUP BY cluster_id
    HAVING coalesce(min(match_probability), 1.0) >= {new_threshold_match_probability}
    """

    sqls.append(
        {"sql": sql, "output_table_name": "__splink__stable_clusters_at_new_threshold"}
    )

    sql = f"""
    select *
    from {cc.templated_name}
    where cluster_id in
    (select cluster_id from __splink__stable_clusters_at_new_threshold)
    """
    sqls.append(
        {"sql": sql, "output_table_name": "__splink__stable_nodes_at_new_threshold"}
    )

    return sqls


def _generate_cluster_comparison_sql(
    all_results: dict[float, SplinkDataFrame],
    unique_id_col: str = "unique_id",
    output_number_of_distinct_clusters_only: bool = False,
):
    thresholds = sorted(all_results.keys())

    def threshold_to_str(x):
        if x == 0.0:
            return "0_0"
        elif x == 1.0:
            return "1_0"
        else:
            return f"{x:.8f}".rstrip("0").replace(".", "_")

    if output_number_of_distinct_clusters_only:
        select_columns = [
            f"COUNT(DISTINCT t{i}.cluster_id) AS distinct_clusters_{threshold_to_str(threshold)}"
            for i, threshold in enumerate(thresholds)
        ]
    else:
        select_columns = [f"t0.{unique_id_col}"] + [
            f"t{i}.cluster_id AS cluster_{threshold_to_str(threshold)}"
            for i, threshold in enumerate(thresholds)
        ]

    from_clause = f"FROM {all_results[thresholds[0]].physical_name} t0"
    join_clauses = [
        f"\nINNER JOIN {all_results[threshold].physical_name} t{i} "
        f"ON t0.{unique_id_col} = t{i}.{unique_id_col}"
        for i, threshold in enumerate(thresholds[1:], start=1)
    ]

    sql = f"""
    SELECT {', '.join(select_columns)}
    {from_clause}
    {' '.join(join_clauses)}
    """

    return sql


def cluster_pairwise_predictions_at_multiple_thresholds(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    match_probability_thresholds: list[float],
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
    output_number_of_distinct_clusters_only: bool = False,
) -> SplinkDataFrame:
    # Strategy to cluster at multiple thresholds:
    # 1. Cluster at the lowest threshold
    # 2. At next threshold, note that some clusters do not need to be recomputed.
    #    Specifically, those where the min probability within the cluster is
    #    greater than this next threshold.
    # 3. Compute remaining clusters which _are_ affected by the next threshold.
    # 4. Repeat for remaining thresholds.

    # Input could either be user data, or a SplinkDataFrame
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

    match_probability_thresholds = sorted(match_probability_thresholds)

    initial_threshold = match_probability_thresholds.pop(0)
    all_results = {}

    edge_id_column_name_left, edge_id_column_name_right = _get_edge_id_column_names(
        node_id_column_name,
        edge_id_column_name_left,
        edge_id_column_name_right,
        db_api,
    )

    # First cluster at the lowest threshold
    cc = cluster_pairwise_predictions_at_threshold(
        nodes=nodes_sdf,
        edges=edges_sdf,
        db_api=db_api,
        node_id_column_name=node_id_column_name,
        edge_id_column_name_left=edge_id_column_name_left,
        edge_id_column_name_right=edge_id_column_name_right,
        threshold_match_probability=initial_threshold,
    )
    all_results[initial_threshold] = cc
    previous_threshold = initial_threshold
    for new_threshold in match_probability_thresholds:
        # Get stable nodes
        pipeline = CTEPipeline([cc, edges_sdf])

        sqls = _calculate_stable_clusters_at_new_threshold(
            edges_sdf=edges_sdf,
            cc=cc,
            node_id_column_name=node_id_column_name,
            edge_id_column_name_left=edge_id_column_name_left,
            edge_id_column_name_right=edge_id_column_name_right,
            previous_threshold_match_probability=previous_threshold,
            new_threshold_match_probability=new_threshold,
        )

        pipeline.enqueue_list_of_sqls(sqls)
        stable_clusters = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        # Derive nodes in play and edges in play by removing stable nodes.  Then
        # run cluster_pairwise_predictions_at_threshold at new threhold

        pipeline = CTEPipeline([nodes_sdf, stable_clusters])
        sql = f"""
        select *
        from {nodes_sdf.templated_name}
        where {node_id_column_name} not in
        (select {node_id_column_name} from {stable_clusters.templated_name})
        """
        pipeline.enqueue_sql(sql, "__splink__nodes_in_play")
        nodes_in_play = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([nodes_in_play, edges_sdf])
        sql = f"""
        select *
        from {edges_sdf.templated_name}
        where {edge_id_column_name_left} in
        (select {node_id_column_name} from {nodes_in_play.templated_name})
        and {edge_id_column_name_right} in
        (select {node_id_column_name} from {nodes_in_play.templated_name})
        """
        pipeline.enqueue_sql(sql, "__splink__edges_in_play")
        edges_in_play = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        marginal_new_clusters = cluster_pairwise_predictions_at_threshold(
            nodes_in_play,
            edges_in_play,
            node_id_column_name=node_id_column_name,
            db_api=db_api,
            threshold_match_probability=new_threshold,
        )

        pipeline = CTEPipeline([stable_clusters, marginal_new_clusters])
        sql = f"""
        SELECT {node_id_column_name}, cluster_id
        FROM {stable_clusters.templated_name}
        UNION ALL
        SELECT {node_id_column_name}, cluster_id
        FROM {marginal_new_clusters.templated_name}
        """

        pipeline.enqueue_sql(sql, "__splink__clusters_at_threshold")

        cc = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        all_results[new_threshold] = cc
        previous_threshold = new_threshold

        edges_in_play.drop_table_from_database_and_remove_from_cache(db_api)
        nodes_in_play.drop_table_from_database_and_remove_from_cache(db_api)
        stable_clusters.drop_table_from_database_and_remove_from_cache(db_api)
        marginal_new_clusters.drop_table_from_database_and_remove_from_cache(db_api)

    sql = _generate_cluster_comparison_sql(
        all_results,
        unique_id_col=node_id_column_name,
        output_number_of_distinct_clusters_only=output_number_of_distinct_clusters_only,
    )
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__clusters_at_all_thresholds")
    joined = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    for v in all_results.values():
        v.drop_table_from_database_and_remove_from_cache(db_api)

    return joined

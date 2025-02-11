from __future__ import annotations

import logging
import math
from typing import Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.input_column import InputColumn
from splink.internals.misc import (
    ascii_uid,
    prob_to_match_weight,
    threshold_args_to_match_prob,
    threshold_args_to_match_prob_list,
)
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


def _get_edge_id_column_names(
    node_id_column_name: str,
    db_api: DatabaseAPISubClass,
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
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
    threshold_match_weight: Optional[float] = None,
) -> SplinkDataFrame:
    """Clusters the pairwise match predictions into groups of connected records using
    the connected components graph clustering algorithm.

    Records with an estimated match probability at or above threshold_match_probability
    are considered to be a match (i.e. they represent the same entity).

    If no match probability or match weight is provided, it is assumed that all edges
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
        threshold_match_weight (Optional[float]): Pairwise comparisons with a
            match_weight at or above this threshold are matched

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
        db_api,
        edge_id_column_name_left,
        edge_id_column_name_right,
    )

    threshold_match_probability = threshold_args_to_match_prob(
        threshold_match_probability, threshold_match_weight
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


def _threshold_to_str(match_probability: float, is_match_weight: bool = False) -> str:
    if is_match_weight:
        if match_probability == 0.0:
            return "mw_minus_inf"
        elif match_probability == 1.0:
            return "mw_inf"
        else:
            weight = prob_to_match_weight(match_probability)
            formatted = f"{weight:.6f}".rstrip("0")
            if formatted.endswith("."):
                formatted = formatted[:-1]
            return f"mw_{formatted.replace('.', '_')}"
    else:
        if match_probability == 0.0:
            return "0_0"
        elif match_probability == 1.0:
            return "1_0"
        formatted = f"{match_probability:.6f}".rstrip("0")
        if formatted.endswith("."):
            formatted = formatted[:-1]
        return f"p_{formatted.replace('.', '_')}"


def _generate_detailed_cluster_comparison_sql(
    all_results: dict[float, SplinkDataFrame],
    unique_id_col: str = "unique_id",
    is_match_weight: bool = False,
) -> str:
    thresholds = sorted(all_results.keys())

    select_columns = [f"t0.{unique_id_col}"] + [
        f"t{i}.cluster_id AS cluster_{_threshold_to_str(threshold, is_match_weight)}"
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


def _get_cluster_stats_sql(cc: SplinkDataFrame) -> list[dict[str, str]]:
    sqls = []
    cluster_sizes_sql = f"""
    SELECT
        cluster_id,
        COUNT(*) AS cluster_size
    FROM {cc.templated_name}
    GROUP BY cluster_id
    """
    sqls.append(
        {"sql": cluster_sizes_sql, "output_table_name": "__splink__cluster_sizes"}
    )

    cluster_stats_sql = """
    SELECT
        COUNT(*) AS num_clusters,
        MAX(cluster_size) AS max_cluster_size,
        AVG(cluster_size) AS avg_cluster_size
    FROM __splink__cluster_sizes
    """
    sqls.append(
        {"sql": cluster_stats_sql, "output_table_name": "__splink__cluster_stats"}
    )

    return sqls


def _threshold_to_weight_for_table(p):
    if p == 0 or p == 1:
        return "NULL"
    else:
        return str(math.log2(p / (1 - p)))


def _generate_cluster_summary_stats_sql(
    all_results: dict[float, SplinkDataFrame],
) -> str:
    thresholds = sorted(all_results.keys())

    select_statements = [
        f"""
        SELECT
            cast({threshold} as float) as threshold_match_probability,
            cast({_threshold_to_weight_for_table(threshold)} as float)
                as threshold_match_weight,
            *
        FROM {all_results[threshold].physical_name}
        """
        for threshold in thresholds
    ]

    sql = "\nUNION ALL\n".join(select_statements)

    return sql


def cluster_pairwise_predictions_at_multiple_thresholds(
    nodes: AcceptableInputTableType,
    edges: AcceptableInputTableType,
    db_api: DatabaseAPISubClass,
    node_id_column_name: str,
    match_probability_thresholds: list[float] | None = None,
    match_weight_thresholds: list[float] | None = None,
    edge_id_column_name_left: Optional[str] = None,
    edge_id_column_name_right: Optional[str] = None,
    output_cluster_summary_stats: bool = False,
) -> SplinkDataFrame:
    """Clusters the pairwise match predictions at multiple thresholds using
    the connected components graph clustering algorithm.

    This function efficiently computes clusters for multiple thresholds by starting
    with the lowest threshold and incrementally updating clusters for higher thresholds.

    If your node and edge column names follow Splink naming conventions, then you can
    omit edge_id_column_name_left and edge_id_column_name_right. For example, if you
    have a table of nodes with a column `unique_id`, it would be assumed that the
    edge table has columns `unique_id_l` and `unique_id_r`.

    Args:
        nodes (AcceptableInputTableType): The table containing node information
        edges (AcceptableInputTableType): The table containing edge information
        db_api (DatabaseAPISubClass): The database API to use for querying
        node_id_column_name (str): The name of the column containing node IDs
        match_probability_thresholds (list[float] | None): List of match probability
            thresholds to compute clusters for
        match_weight_thresholds (list[float] | None): List of match weight thresholds
            to compute clusters for
        edge_id_column_name_left (Optional[str]): The name of the column containing
            left edge IDs. If not provided, assumed to be f"{node_id_column_name}_l"
        edge_id_column_name_right (Optional[str]): The name of the column containing
            right edge IDs. If not provided, assumed to be f"{node_id_column_name}_r"
        output_cluster_summary_stats (bool): If True, output summary statistics
            for each threshold instead of full cluster information

    Returns:
        SplinkDataFrame: A SplinkDataFrame containing cluster information for all
            thresholds. If output_cluster_summary_stats is True, it contains summary
            statistics (number of clusters, max cluster size, avg cluster size) for
            each threshold.

    Examples:
        ```python
        from splink import DuckDBAPI
        from splink.clustering import (
            cluster_pairwise_predictions_at_multiple_thresholds
        )

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

        thresholds = [0.5, 0.7, 0.9]

        cc = cluster_pairwise_predictions_at_multiple_thresholds(
            nodes,
            edges,
            node_id_column_name="my_id",
            edge_id_column_name_left="n_1",
            edge_id_column_name_right="n_2",
            db_api=db_api,
            match_probability_thresholds=thresholds,
        )

        cc.as_duckdbpyrelation()
        ```
    """

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

    is_match_weight = (
        match_weight_thresholds is not None and match_probability_thresholds is None
    )

    match_probability_thresholds = threshold_args_to_match_prob_list(
        match_probability_thresholds, match_weight_thresholds
    )

    if match_probability_thresholds is None or len(match_probability_thresholds) == 0:
        raise ValueError(
            "Must provide either match_probability_thresholds "
            "or match_weight_thresholds"
        )

    initial_threshold = match_probability_thresholds.pop(0)
    all_results = {}

    edge_id_column_name_left, edge_id_column_name_right = _get_edge_id_column_names(
        node_id_column_name,
        db_api,
        edge_id_column_name_left,
        edge_id_column_name_right,
    )
    logger.info(f"--------Clustering at threshold {initial_threshold}--------")
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

    if output_cluster_summary_stats:
        pipeline = CTEPipeline([cc])
        sqls = _get_cluster_stats_sql(cc)
        pipeline.enqueue_list_of_sqls(sqls)
        cc_summary = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        all_results[initial_threshold] = cc_summary
    else:
        all_results[initial_threshold] = cc

    previous_threshold = initial_threshold
    for new_threshold in match_probability_thresholds:
        # Get stable nodes
        logger.info(f"--------Clustering at threshold {new_threshold}--------")
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
            edge_id_column_name_left=edge_id_column_name_left,
            edge_id_column_name_right=edge_id_column_name_right,
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

        previous_cc = cc
        cc = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        previous_threshold = new_threshold

        edges_in_play.drop_table_from_database_and_remove_from_cache()
        nodes_in_play.drop_table_from_database_and_remove_from_cache()
        stable_clusters.drop_table_from_database_and_remove_from_cache()
        marginal_new_clusters.drop_table_from_database_and_remove_from_cache()

        if output_cluster_summary_stats:
            pipeline = CTEPipeline([cc])
            sqls = _get_cluster_stats_sql(cc)
            pipeline.enqueue_list_of_sqls(sqls)
            cc_summary = db_api.sql_pipeline_to_splink_dataframe(pipeline)
            all_results[new_threshold] = cc_summary
            previous_cc.drop_table_from_database_and_remove_from_cache()
        else:
            all_results[new_threshold] = cc

    if output_cluster_summary_stats:
        sql = _generate_cluster_summary_stats_sql(all_results)
    else:
        sql = _generate_detailed_cluster_comparison_sql(
            all_results,
            unique_id_col=node_id_column_name,
            is_match_weight=is_match_weight,
        )

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__clusters_at_all_thresholds")
    joined = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    for v in all_results.values():
        v.drop_table_from_database_and_remove_from_cache()
    cc.drop_table_from_database_and_remove_from_cache()

    return joined

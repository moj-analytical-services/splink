from __future__ import annotations

import logging
import time
from typing import Optional

from splink.internals.connected_components import solve_connected_components
from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


def solve_complete_linkage(
    nodes_table: SplinkDataFrame,
    edges_table: SplinkDataFrame,
    node_id_column_name: str,
    edge_id_column_name_left: str,
    edge_id_column_name_right: str,
    db_api: DatabaseAPISubClass,
    threshold_match_probability: Optional[float],
) -> SplinkDataFrame:
    """Complete-linkage clustering algorithm.

    Unlike connected components, which merges clusters whenever ANY path of
    high-probability edges connects two nodes, complete-linkage only places two
    nodes in the same cluster when no directly-observed edge within the cluster
    has a match probability below the threshold.

    This prevents the "chaining" problem where A≈B and B≈C leads A and C to be
    merged even when A-C is a known poor match.

    A cluster is valid if: for every pair of nodes (u, v) within the cluster
    where an edge exists in edges_table, that edge has
    match_probability >= threshold_match_probability.

    Algorithm:
        1. Build the working edge set E_work from edges at or above threshold.
        2. Run connected components on E_work to get candidate clusters.
        3. Find "conflict" edges: edges in edges_table below threshold whose
           endpoints both fall in the same cluster.
        4. If no conflicts, return the current clusters.
        5. For each cluster containing at least one conflict, remove the
           minimum-probability E_work edge within that cluster (weakest link).
        6. Return to step 2.

    Each outer iteration removes at least one edge per conflicted cluster, so
    the algorithm is guaranteed to terminate.

    Args:
        nodes_table: Nodes to cluster.
        edges_table: All pairwise edges, including those below threshold.
            Must include a ``match_probability`` column when
            ``threshold_match_probability`` is not None.
        node_id_column_name: Column name for node IDs in ``nodes_table``.
        edge_id_column_name_left: Column name for the left node ID in
            ``edges_table``.
        edge_id_column_name_right: Column name for the right node ID in
            ``edges_table``.
        db_api: Database API.
        threshold_match_probability: Minimum match probability for an edge to be
            included in a cluster.  When None every edge is treated as a match
            and the result is identical to connected components.

    Returns:
        SplinkDataFrame with columns ``(node_id_column_name, cluster_id)``.
    """
    if threshold_match_probability is None:
        return solve_connected_components(
            nodes_table=nodes_table,
            edges_table=edges_table,
            node_id_column_name=node_id_column_name,
            edge_id_column_name_left=edge_id_column_name_left,
            edge_id_column_name_right=edge_id_column_name_right,
            db_api=db_api,
            threshold_match_probability=None,
        )

    uid = ascii_uid(8)

    # Warn if the edges table contains no below-threshold edges.  This is the
    # most common mistake: the user pre-filtered their predictions to
    # above-threshold only (e.g. linker.predict(threshold_match_probability=X))
    # before passing them here.  In that case there are no conflict edges for
    # the algorithm to detect, so it will always terminate on the first
    # iteration and produce the same result as connected components — silently.
    pipeline = CTEPipeline([edges_table])
    sql = f"""
    select count(*) as below_threshold_count
    from {edges_table.templated_name}
    where match_probability < {threshold_match_probability}
    """
    pipeline.enqueue_sql(sql, f"__splink__cl_below_threshold_check_{uid}")
    below_df = db_api.sql_pipeline_to_splink_dataframe(pipeline, use_cache=False)
    below_count = below_df.as_record_dict()[0]["below_threshold_count"]
    below_df.drop_table_from_database_and_remove_from_cache()

    if below_count == 0:
        logger.warning(
            "Complete linkage: the edges table contains no edges below the "
            f"threshold ({threshold_match_probability}). Conflict detection "
            "requires below-threshold edges to be present — without them the "
            "result is identical to connected components. "
            "Ensure you pass the full (unfiltered) predictions table, e.g. "
            "linker.predict() with no threshold, or a threshold lower than "
            f"{threshold_match_probability}."
        )

    # Build E_work: the working set of edges (only those at or above threshold).
    # Below-threshold edges in edges_table are "conflict" edges — they remain
    # visible to the algorithm for conflict detection but are never added to
    # E_work and therefore never form links.
    pipeline = CTEPipeline([edges_table])
    sql = f"""
    select
        {edge_id_column_name_left},
        {edge_id_column_name_right},
        match_probability
    from {edges_table.templated_name}
    where match_probability >= {threshold_match_probability}
    """
    pipeline.enqueue_sql(sql, f"__splink__cl_e_work_initial_{uid}")
    e_work = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    iteration = 0
    while True:
        start_time = time.time()
        iteration += 1

        # Step 2: find connected components of the current working edge set.
        # Pass threshold_match_probability=None because e_work is already
        # filtered to above-threshold edges only.
        cc = solve_connected_components(
            nodes_table=nodes_table,
            edges_table=e_work,
            node_id_column_name=node_id_column_name,
            edge_id_column_name_left=edge_id_column_name_left,
            edge_id_column_name_right=edge_id_column_name_right,
            db_api=db_api,
            threshold_match_probability=None,
        )

        # Step 3: count conflict edges — below-threshold edges whose endpoints
        # are both in the same cluster.
        pipeline = CTEPipeline([edges_table, cc])
        sql = f"""
        select count(*) as conflict_count
        from {edges_table.templated_name} e
        inner join {cc.templated_name} cl
            on e.{edge_id_column_name_left} = cl.{node_id_column_name}
        inner join {cc.templated_name} cr
            on e.{edge_id_column_name_right} = cr.{node_id_column_name}
        where e.match_probability < {threshold_match_probability}
          and cl.cluster_id = cr.cluster_id
        """
        pipeline.enqueue_sql(sql, "__splink__cl_conflict_count")
        conflict_count_df = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )
        conflict_count = conflict_count_df.as_record_dict()[0]["conflict_count"]
        conflict_count_df.drop_table_from_database_and_remove_from_cache()

        elapsed = time.time() - start_time
        logger.info(
            f"Complete linkage iteration {iteration}: "
            f"{conflict_count} conflicting edge(s) remaining. "
            f"({elapsed:.2f}s)"
        )
        if iteration > 20:
            logger.warning(
                f"Complete linkage is on iteration {iteration} with "
                f"{conflict_count} conflicting edge(s) still remaining. "
                "This may indicate many conflicts in your data — consider "
                "checking your match probability threshold or model calibration."
            )

        # Step 4: done when no conflicts remain.
        if conflict_count == 0:
            break

        # Step 5: for each conflicted cluster, remove its minimum-probability
        # E_work edge (the weakest link).  Removing the weakest link is the
        # most conservative intervention — it preserves as many high-confidence
        # edges as possible while making progress toward separating conflicted
        # node pairs.
        #
        # Because every E_work edge connects two nodes in the same CC cluster
        # (by definition of connected components), joining on the left node
        # alone is sufficient to retrieve the cluster_id for an E_work edge.
        pipeline = CTEPipeline([edges_table, cc, e_work])

        # Clusters that contain at least one conflict edge.
        sql = f"""
        select distinct cl.cluster_id
        from {edges_table.templated_name} e
        inner join {cc.templated_name} cl
            on e.{edge_id_column_name_left} = cl.{node_id_column_name}
        inner join {cc.templated_name} cr
            on e.{edge_id_column_name_right} = cr.{node_id_column_name}
        where e.match_probability < {threshold_match_probability}
          and cl.cluster_id = cr.cluster_id
        """
        pipeline.enqueue_sql(sql, "__splink__cl_conflicted_clusters")

        # Annotate E_work edges with their cluster_id.
        sql = f"""
        select
            ew.{edge_id_column_name_left},
            ew.{edge_id_column_name_right},
            ew.match_probability,
            cl.cluster_id
        from {e_work.templated_name} ew
        inner join {cc.templated_name} cl
            on ew.{edge_id_column_name_left} = cl.{node_id_column_name}
        """
        pipeline.enqueue_sql(sql, "__splink__cl_e_work_labeled")

        # Rank E_work edges within each conflicted cluster by match_probability
        # ascending (rn = 1 is the weakest link to remove).  The secondary sort
        # on the ID columns gives a deterministic result when probabilities tie.
        sql = f"""
        select
            {edge_id_column_name_left},
            {edge_id_column_name_right},
            match_probability,
            cluster_id,
            row_number() over (
                partition by cluster_id
                order by
                    match_probability asc,
                    {edge_id_column_name_left} asc,
                    {edge_id_column_name_right} asc
            ) as rn
        from __splink__cl_e_work_labeled
        where cluster_id in (
            select cluster_id from __splink__cl_conflicted_clusters
        )
        """
        pipeline.enqueue_sql(sql, "__splink__cl_e_work_ranked")

        # The single weakest-link edge per conflicted cluster.
        sql = f"""
        select {edge_id_column_name_left}, {edge_id_column_name_right}
        from __splink__cl_e_work_ranked
        where rn = 1
        """
        pipeline.enqueue_sql(sql, "__splink__cl_edges_to_remove")

        # New E_work excludes the removed edges.  A left-join / IS NULL filter
        # is used for broad SQL-dialect compatibility.
        sql = f"""
        select
            ew.{edge_id_column_name_left},
            ew.{edge_id_column_name_right},
            ew.match_probability
        from {e_work.templated_name} ew
        left join __splink__cl_edges_to_remove er
            on  ew.{edge_id_column_name_left}  = er.{edge_id_column_name_left}
            and ew.{edge_id_column_name_right} = er.{edge_id_column_name_right}
        where er.{edge_id_column_name_left} is null
        """
        pipeline.enqueue_sql(sql, f"__splink__cl_e_work_{iteration}")
        new_e_work = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        # The current cc is not the final result; drop it before the next round.
        cc.drop_table_from_database_and_remove_from_cache()
        old_e_work = e_work
        e_work = new_e_work
        old_e_work.drop_table_from_database_and_remove_from_cache()

    # cc is now the final conflict-free clustering.  Clean up e_work.
    e_work.drop_table_from_database_and_remove_from_cache()
    return cc

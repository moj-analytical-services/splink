# This sql code for solving connected components takes inspiration
# from the following paper: https://arxiv.org/pdf/1802.09478.pdf

# While we haven't been able to implement the solution presented
# by the paper - due to SQL backend restrictions with UDFs, -
# we have been able to use the paper to further our understanding
# of the problem and come to a working solution.

# See also https://github.com/RobinL/clustering_in_sql
# and https://www.robinlinacre.com/connected_components/

from __future__ import annotations

import logging
import time
from typing import Optional

from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


def _cc_generate_representatives_loop_cond(
    prev_representatives: str, filtered_neighbours: str
) -> str:
    """SQL for Connected components main loop.

    Takes our core neighbours table (this is constant), and
    joins on the current representatives table from the
    previous iteration by joining on information about each node's
    neighbours representatives.

    So, reusing the same summary logic mentioned above, if we know that B
    is represented by A (B -> A) and C is represented by B (C -> B),
    then we can join (B -> A) onto (C -> B) to conclude that (C -> A).

    Doing this iteratively eventually allows us to climb up the ladder through
    all of our neighbours' representatives to a solution.

    The key difference between this function and 'cc_update_neighbours_first_iter',
    is the usage of 'needs_updating'.

    The logic behind 'needs_updating' is summarised in
    'cc_update_representatives_first_iter' and it can be used here to reduce our
    neighbours table to only those nodes that need updating.
    """

    sql = f"""
    select

    old_rep,
    min(representative) as representative,
    min(stable) = 1 as stable

    from
    (

        select

            node_rep as old_rep,
            neighbour_rep as representative,
            0 as stable

        from {filtered_neighbours}

        union all

        select

            representative as old_rep,
            representative,
            1 as stable

        from {prev_representatives}

    )
    group by old_rep
    """

    return sql


def _cc_update_representatives_loop_cond(
    prev_representatives: str,
) -> str:
    """SQL to update our representatives table - while loop condition.

    Reorganises our representatives output generated in
    cc_generate_representatives_loop_cond() and isolates 'needs_updating',
    which indicates whether all representatives have 'settled' (i.e.
    no change from previous iteration).
    """

    sql = f"""
    select

        repr.node_id,
        r.representative,
        r.stable

    from r

    left join {prev_representatives} as repr
    on r.old_rep = repr.representative
    """

    return sql


def _cc_assess_exit_condition(neighbours_name: str) -> str:
    """SQL exit condition for our Connected Components algorithm.

    Where 'needs_updating' (summarised in 'cc_update_representatives_first_iter')
    it indicates that some nodes still require updating and have not yet
    settled.
    """

    sql = f"""
    select count(*) as count_of_edges_needing_processing
    from {neighbours_name}
    """

    return sql


def solve_connected_components(
    nodes_table: SplinkDataFrame,
    edges_table: SplinkDataFrame,
    node_id_column_name: str,
    edge_id_column_name_left: str,
    edge_id_column_name_right: str,
    db_api: DatabaseAPISubClass,
    threshold_match_probability: Optional[float],
) -> SplinkDataFrame:
    """Connected Components main algorithm.

    This function helps cluster your linked (or deduped) records
    into single groups, which can then be more easily visualised.

    Args:
        linker:
            Splink linker object. For more, see splink.linker.

        edges_table (SplinkDataFrame):
            Splink dataframe containing our edges dataframe to be connected.


    Returns:
        SplinkDataFrame: A dataframe containing the connected components list
        for your link or dedupe job.

    """

    # Unlike most Splink SQL generaiton, the templated_name of the edges table
    # and the nodes table are not known as fixed strings because they
    # can be used provided

    pipeline = CTEPipeline([edges_table])

    match_prob_expr = f"where match_probability >= {threshold_match_probability}"
    if threshold_match_probability is None:
        match_prob_expr = ""

    # add reverse edges so these can also be considered by the algorithm
    sql = f"""
    select
        {edge_id_column_name_left} as node_rep,
        {edge_id_column_name_left} as node_id,
        {edge_id_column_name_right} as neighbour_rep,
        {edge_id_column_name_right} as neighbour
    from {edges_table.templated_name}
    {match_prob_expr}

    union all

    select
        {edge_id_column_name_right} as node_rep,
        {edge_id_column_name_right} as node_id,
        {edge_id_column_name_left} as neighbour_rep,
        {edge_id_column_name_left} as neighbour
    from {edges_table.templated_name}
    {match_prob_expr}
    """

    pipeline.enqueue_sql(sql, "__splink__df_neighbours")
    neighbours = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    # Create our initial representatives table
    # use distinct to account for nodes tables with multiple rows per
    # node_id
    pipeline = CTEPipeline([nodes_table])
    sql = f"""
    select distinct
        {node_id_column_name} as node_id,
        {node_id_column_name} as representative,
        0 as stable
    from {nodes_table.templated_name}
    """
    pipeline.enqueue_sql(sql, "__splink__df_representatives")

    representatives = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    prev_representatives_table = representatives

    # Loop while our representative table still has unsettled nodes
    # (nodes where the representative has changed since the last iteration)
    converged_clusters_tables = []
    filtered_neighbours = neighbours

    iteration, needs_updating_count = 0, 1
    while needs_updating_count > 0:
        start_time = time.time()
        iteration += 1

        # Loop summary:
        # 1. Update the representatives by following links from current reps
        #    to their neighbours' representatives, and taking the minimum as the
        #    new rep. This is a concordance of old_reps and new representatives,
        #    and also includes a stable flag which is 1 if that cluster has no more
        #    links to follow, and 0 otherwise.
        # 2. Update the concordance of node_ids and representatives by joining on
        #    old_rep.
        # 3. Split out the stable and unstable clusters, the stable ones can be put
        #    aside until the end, while the unstable clusters progress to the next
        #    iteration.
        # 4. Update the neighbours table with the new node and neighbour
        #    representatives, where we can filter out edges within the same cluster.
        #    If there are no more edges between different clusters then we can end
        #    the loop.

        # 1. find rep updates
        pipeline = CTEPipeline([filtered_neighbours, prev_representatives_table])
        sql = _cc_generate_representatives_loop_cond(
            prev_representatives_table.templated_name,
            filtered_neighbours.templated_name,
        )
        pipeline.enqueue_sql(sql, "r")

        # 2. match node_ids with their new reps
        sql = _cc_update_representatives_loop_cond(
            prev_representatives_table.templated_name
        )

        pipeline.enqueue_sql(sql, f"__splink__representatives_{iteration}")
        representatives = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        # 3. update stable and unstable representatives
        pipeline = CTEPipeline([representatives])
        sql = f"""
        select *
        from {representatives.templated_name}
        where stable
        """
        pipeline.enqueue_sql(sql, f"__splink__representatives_stable_{iteration}")
        converged_clusters = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        converged_clusters_tables.append(converged_clusters)

        pipeline = CTEPipeline([representatives])
        sql = f"""
        select *
        from {representatives.templated_name}
        where not stable
        """
        pipeline.enqueue_sql(sql, f"__splink__representatives_unstable_{iteration}")
        unstable_clusters = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        # 4. filter neighbours and check if any links to process remain
        pipeline = CTEPipeline([representatives, filtered_neighbours])
        sql = f"""
        select
            l.representative as node_rep,
            n.node_id,
            n.neighbour,
            r.representative as neighbour_rep
        from {representatives.templated_name} as l
        join {neighbours.templated_name} as n
        on l.node_id = n.node_id
        join {representatives.templated_name} as r
        on n.neighbour = r.node_id
        where node_rep <> neighbour_rep
        """

        pipeline.enqueue_sql(sql, f"__splink__filtered_neighbours_{iteration}")

        neighbours = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        filtered_neighbours.drop_table_from_database_and_remove_from_cache()
        filtered_neighbours = neighbours

        representatives.drop_table_from_database_and_remove_from_cache()
        prev_representatives_table.drop_table_from_database_and_remove_from_cache()
        prev_representatives_table = unstable_clusters

        # 5. check exit condition
        pipeline = CTEPipeline([filtered_neighbours])
        sql = _cc_assess_exit_condition(
            filtered_neighbours.templated_name
        )
        pipeline.enqueue_sql(sql, "__splink__root_rows")
        root_rows_df = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        root_rows = root_rows_df.as_record_dict()
        root_rows_df.drop_table_from_database_and_remove_from_cache()
        needs_updating_count = root_rows[0]["count_of_edges_needing_processing"]
        logger.info(
            f"Completed iteration {iteration}, "
            f"num edges remaining to process: {needs_updating_count}"
        )
        end_time = time.time()
        logger.log(15, f"    Iteration time: {end_time - start_time} seconds")

    converged_clusters_tables.append(unstable_clusters)
    filtered_neighbours.drop_table_from_database_and_remove_from_cache()

    pipeline = CTEPipeline()

    sql = " UNION ALL ".join(
        [
            f"""select node_id as {node_id_column_name}, representative as cluster_id
            from {t.physical_name}"""
            for t in converged_clusters_tables
        ]
    )

    pipeline.enqueue_sql(sql, "__splink__clustering_output_final")

    final_result = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    representatives.drop_table_from_database_and_remove_from_cache()
    neighbours.drop_table_from_database_and_remove_from_cache()

    for t in converged_clusters_tables:
        t.drop_table_from_database_and_remove_from_cache()

    return final_result

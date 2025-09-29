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


def _cc_generate_neighbours_representation() -> str:
    """SQL to generate all the 'neighbours' of each input node.

    The 'neighbour' of a node is any other node that is connected to the original node
    within the graph.  'Connected' means that at the threshold match probability,
    the nodes are considered to be a match (i.e. the nodes are estimated to
    be same entity)

    This table differs to the edges table in two respects:
    1.  Unlike the edges table, it's not guaranteed that the node (left hand side)
        has a lower id than the neighbour.  That is, the left column (node_id) contains
        all the nodes, not just the ones with a higher id on the right hand side
    2. The table contains all nodes, even those with no edges (these are represented)
        as a 'self link' i.e. ID1 -> ID1, ensuring they are present in the final
        clusters table.
    """

    sql = """
    select n.node_id,
        e_l.node_id_r as neighbour
    from nodes_ids_only as n

    left join __splink__df_edges_with_self_loops as e_l
        on n.node_id = e_l.node_id_l

    UNION

    select n.node_id,
        coalesce(e_r.node_id_l, n.node_id) as neighbour
    from nodes_ids_only as n

    left join __splink__df_edges_with_self_loops as e_r
        on n.node_id = e_r.node_id_r
    """

    return sql


def _cc_generate_initial_representatives_table() -> str:
    """SQL to generate our initial "representatives" table.

    The 'representative' column will eventually become the cluster ID.

    As outlined in the paper quoted at the top:

    '...begin by choosing for each vertex (node) a representatative by picking the
    vertex (node) with the minimum id amongst itself and its neighbours'.

    e.g. node ids 1, 2 and 3 may all have representative 2, indicating
    they are a cluster.

    This is done initially by grouping on our neighbours table
    and finding the minimum neighbour for each node.
    """

    sql = """
    select
        neighbours.node_id,
        min(neighbour) as representative

    from __splink__df_neighbours as neighbours
    group by node_id
    order by node_id
    """

    return sql


def _cc_update_neighbours_first_iter() -> str:
    """SQL to update our neighbours table - first iteration only.

    Takes our initial neighbours table, join on the representatives table
    and recalculates the mimumum representative for each node.

    This works by joining on the current known representative for each node's
    neighbours.

    i.e. rather than looking at a node's minimum representative, we look at the node's
    neighbour's minimum representative.

    So, if we know that B is represented by A (B -> A) and C is represented by B
    (C -> B), then we can join on B to conclude that (C -> A).
    """

    sql = """
    select
        neighbours.node_id,
        min(representatives.representative) as representative

    from __splink__df_neighbours as neighbours
    left join representatives
    on neighbours.neighbour = representatives.node_id
        group by neighbours.node_id
        order by neighbours.node_id
    """

    return sql


def _cc_update_representatives_first_iter() -> str:
    """SQL to update our representatives table - first iteration only.

    From here, standardised code can be used inside a while loop,
    as the representatives table no longer needs generating.

    This is only used for the first iteration as we

    In this SQL, we also generate "needs_updating", which is a boolean
    that indicates whether the current representative differs
    from the previous representative.

    This value is used extensively to speed up our while loop and as
    an exit condition.
    """

    sql = """
    select
        n.node_id,
        n.representative,
        n.representative <> repr.representative as needs_updating
    from neighbours_first_iter as n
    inner join representatives as repr
    on n.node_id = repr.node_id
    """

    return sql


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

    source.node_id,
    min(source.representative) as representative

    from
    (

        select

            neighbours.node_id,
            repr_neighbour.representative as representative

        from {filtered_neighbours} as neighbours

        inner join {prev_representatives} as repr_neighbour
        on neighbours.neighbour = repr_neighbour.node_id

        where
            repr_neighbour.needs_updating

        UNION ALL

        select

            node_id,
            representative

        from {prev_representatives}

    ) AS source
    group by source.node_id
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

        r.node_id,
        r.representative,
        r.representative <> repr.representative as needs_updating

    from r

    left join {prev_representatives} as repr
    on r.node_id = repr.node_id
        """

    return sql


def _cc_assess_exit_condition(representatives_name: str) -> str:
    """SQL exit condition for our Connected Components algorithm.

    Where 'needs_updating' (summarised in 'cc_update_representatives_first_iter')
    it indicates that some nodes still require updating and have not yet
    settled.
    """

    sql = f"""
            select count(*) as count_of_nodes_needing_updating
            from {representatives_name}
            where needs_updating
        """

    return sql


def _cc_find_converged_nodes(
    representatives_name: str,
    neighbours_name: str,
    iteration: int,
) -> list[dict[str, str]]:
    """SQL to find nodes that have converged so are part of a stable cluster.
    These can be removed 'from play' to slim down tables and make the algorithm
    run faster.

    Args:
        representatives_name: The name of the representatives table.
        neighbours_name: The name of the neighbours table.
        iteration: The iteration of the algorithm

    Returns:
        str: SQL query to find unconverged nodes.
    """

    # Take nodes, and find neighbours to the nodes (follow edges in both directions)
    # For each neighbour to the node, find its representative
    # Does that lead us back to the same cluster?  Tf not there is a neighbour
    # outside of the cluster
    sqls = []

    sql_non_stable = f"""
    SELECT DISTINCT r.representative
    FROM {representatives_name} r
    JOIN {neighbours_name} n ON r.node_id = n.node_id
    JOIN {representatives_name} r2 ON n.neighbour = r2.node_id
    WHERE r.representative != r2.representative
    """

    sqls.append(
        {
            "sql": sql_non_stable,
            "output_table_name": "non_stable_representatives",
        }
    )

    sql_stable = f"""
    SELECT *
        FROM {representatives_name} as r
        WHERE NOT EXISTS (
            SELECT 1 FROM non_stable_representatives as nsr
            WHERE r.representative = nsr.representative
        )
        """
    sqls.append(
        {
            "sql": sql_stable,
            "output_table_name": f"__splink__representatives_stable_{iteration}",
        }
    )

    return sqls


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
        {edge_id_column_name_left} as rep_l,
        {edge_id_column_name_left} as node_id_l,
        {edge_id_column_name_right} as rep_r,
        {edge_id_column_name_right} as node_id_r
    from {edges_table.templated_name}
    {match_prob_expr}

    union all

    select
        {edge_id_column_name_right} as rep_l,
        {edge_id_column_name_right} as node_id_l,
        {edge_id_column_name_left} as rep_r,
        {edge_id_column_name_left} as node_id_r
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
        # 1. Find stable clusters and remove from representatives table
        #    Stable clusters are those where a set of nodes are within the same cluster
        #    and those nodes have no neighbours outside of their cluster.
        #    Add to list of converged clusters.
        # 2. Update representatives table by following links from current reps
        #    to their neighbours, and recalculating min representative
        # 3. Join on the representatives table from the previous iteration
        #    to create the "needs_updating" column based on whether rep has changed
        # 4. Assess if any representatives changed between iterations, exit if not.

        # 1. find rep updates
        pipeline = CTEPipeline([filtered_neighbours, prev_representatives_table])
        sql = f"""
        select 
            old_rep,
            min(representative) as representative
        from (
            select 
                rep_l as old_rep,
                rep_r as representative
            from {neighbours.templated_name}

            union all

            select 
                representative as old_rep,
                representative as representative
            from {prev_representatives_table.templated_name}
        )
        group by old_rep
        """

        pipeline.enqueue_sql(sql, f"__splink__rep_updates_{iteration}")

        # 2. match node_ids with their new reps
        sql = f"""
        select
            node_id,
            u.representative
        from {prev_representatives_table.templated_name} as p
        join __splink__rep_updates_{iteration} as u
        on p.representative = u.old_rep
        """

        pipeline.enqueue_sql(sql, f"__splink__representatives_{iteration}")
        representatives = db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([representatives, filtered_neighbours])
        # Update table reference
        prev_representatives_table.drop_table_from_database_and_remove_from_cache()
        prev_representatives_table = representatives

        # 3. filter neighbours and check if any links to process remain
        sql = f"""
        select
            l.representative as rep_l,
            n.node_id_l,
            n.node_id_r,
            r.representative as rep_r,
        from {representatives.templated_name} as l
        join {neighbours.templated_name} as n
        on l.node_id = n.node_id_l
        join {representatives.templated_name} as r
        on n.node_id_r = r.node_id
        where rep_l <> rep_r
        """

        pipeline.enqueue_sql(sql, f"__splink__filtered_neighbours_{iteration}")

        neighbours = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        filtered_neighbours.drop_table_from_database_and_remove_from_cache()
        filtered_neighbours = neighbours

        # 4. check exit condition (any edges left to process)
        pipeline = CTEPipeline([filtered_neighbours])
        sql = f"""
        select count(*) as count_of_nodes_needing_updating
        from {filtered_neighbours.templated_name}
        """
        pipeline.enqueue_sql(sql, "__splink__root_rows")
        root_rows_df = db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        root_rows = root_rows_df.as_record_dict()
        root_rows_df.drop_table_from_database_and_remove_from_cache()
        needs_updating_count = root_rows[0]["count_of_nodes_needing_updating"]
        logger.info(
            f"Completed iteration {iteration}, "
            f"num representatives needing updating: {needs_updating_count}"
        )
        end_time = time.time()
        logger.log(15, f"    Iteration time: {end_time - start_time} seconds")

    filtered_neighbours.drop_table_from_database_and_remove_from_cache()

    pipeline = CTEPipeline()

    sql = f"""
    select
        node_id as {node_id_column_name},
        representative as cluster_id
    from {representatives.physical_name}
    """

    pipeline.enqueue_sql(sql, "__splink__clustering_output_final")

    final_result = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    representatives.drop_table_from_database_and_remove_from_cache()
    neighbours.drop_table_from_database_and_remove_from_cache()

    return final_result

# This sql code for solving connected components takes inspiration
# from the following paper: https://arxiv.org/pdf/1802.09478.pdf

# While we haven't been able to implement the solution presented
# by the paper - due to SQL backend restrictions with UDFs, -
# we have been able to use the paper to further our understanding
# of the problem and come to a working solution.

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
import time
from typing import TYPE_CHECKING

import logging
from .unique_id_concat import (
    _composite_unique_id_from_nodes_sql,
    _composite_unique_id_from_edges_sql,
)

from .splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from .linker import Linker

logger = logging.getLogger(__name__)


def _cc_create_nodes_table(linker: "Linker", edge_table, generated_graph=False):

    """SQL to create our connected components nodes table.

    From our edges table, create a nodes table.

    This captures ALL nodes in our edges representation
    as a single columnar list.

    This logic can be shortcut by using the unique
    id column found in __splink__df_concat_with_tf.
    """

    uid_cols = linker._settings_obj._unique_id_input_columns
    uid_concat = _composite_unique_id_from_nodes_sql(uid_cols)

    if generated_graph:
        sql = f"""
        select unique_id_l as node_id
            from {edge_table}

            UNION

        select unique_id_r as node_id
            from {edge_table}
        """
    else:

        sql = f"""
        select {uid_concat} as node_id
        from __splink__df_concat_with_tf
        """

    return sql


def _cc_generate_neighbours_representation(edges_table):

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

    sql = f"""
    select n.node_id,
        e_l.unique_id_r as neighbour
    from nodes as n

    left join {edges_table} as e_l
        on n.node_id = e_l.unique_id_l

    UNION ALL

    select n.node_id,
        coalesce(e_r.unique_id_l, n.node_id) as neighbour
    from nodes as n

    left join {edges_table} as e_r
        on n.node_id = e_r.unique_id_r
    """

    return sql


def _cc_generate_initial_representatives_table(neighbours_table):

    """SQL to generate our initial "representatives" table.

    The 'representative' column will eventually become the cluster ID.

    As outlined in the paper quoted at the top:

    '...begin by choosing for each vertex (node) a representatative by picking the
    vertex (node) with the minimum id amongst itself and its neighbours'.

    e.g. node ids 1, 2 and 3 may all have representative 2, indicating
    they are a cluster.

    This is done initially by grouping on our neighbours table
    and finding the minimum representative for each node.
    """

    sql = f"""
    select
        neighbours.node_id,
        min(neighbour) as representative

    from {neighbours_table} as neighbours
    group by node_id
    order by node_id
    """

    return sql


def _cc_update_neighbours_first_iter(neighbours_table):

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

    sql = f"""
    select
        neighbours.node_id,
        min(representatives.representative) as representative

    from {neighbours_table} as neighbours
    left join representatives
    on neighbours.neighbour = representatives.node_id
        group by neighbours.node_id
        order by neighbours.node_id
    """

    return sql


def _cc_update_representatives_first_iter():

    """SQL to update our representatives table - first iteration only.

    From here, standardised code can be used inside a while loop,
    as the representatives table no longer needs generating.

    This is only used for the first iteration as we

    In this SQL, we also generate "rep_match", which is a boolean
    that indicates whether the current representative differs
    from the previous representative.

    This value is used extensively to speed up our while loop and as
    an exit condition.
    """

    sql = """
    select
        n.node_id,
        n.representative,
        n.representative <> repr.representative as rep_match
    from neighbours_first_iter as n
    left join representatives as repr
    on n.node_id = repr.node_id
    """

    return sql


def _cc_generate_representatives_loop_cond(
    neighbours_table,
    prev_representatives,
):

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
    is the usage of 'rep_match'.

    The logic behind 'rep_match' is summarised in 'cc_update_representatives_first_iter'
    and it can be used here to reduce our neighbours table to only those nodes that need
    updating.
    """

    sql = f"""
    select

    node_id,
    min(representative) as representative

    from
    (

        select

            neighbours.node_id,
            repr_neighbour.representative as representative

        from {neighbours_table} as neighbours

        left join {prev_representatives} as repr_neighbour
        on neighbours.neighbour = repr_neighbour.node_id

        where
            repr_neighbour.rep_match

        UNION ALL

        select

            node_id,
            representative

        from {prev_representatives}

    )
    group by node_id
        """

    return sql


def _cc_update_representatives_loop_cond(
    prev_representatives,
):

    """SQL to update our representatives table - while loop condition.

    Reorganises our representatives output generated in
    cc_generate_representatives_loop_cond() and isolates 'rep_match',
    which indicates whether all representatives have 'settled' (i.e.
    no change from previous iteration).
    """

    sql = f"""
    select

        r.node_id,
        r.representative,
        r.representative <> repr.representative as rep_match

    from r

    left join {prev_representatives} as repr
    on r.node_id = repr.node_id
        """

    return sql


def _cc_assess_exit_condition(representatives_name):

    """SQL exit condition for our Connected Components algorithm.

    Where 'rep_match' (summarised in 'cc_update_representatives_first_iter')
    it indicates that some nodes still require updating and have not yet
    settled.
    """

    sql = f"""
            select count(*) as count
            from {representatives_name}
            where rep_match
        """

    return sql


def _cc_create_unique_id_cols(
    linker: "Linker", df_predict: SplinkDataFrame, match_probability_threshold
):

    """Create SQL to pull unique ID columns for connected components.

    Takes the output of linker.predict() and either creates unique IDs for
    our linked dataframes, if we are performing a link job, or pulls out
    the unique ID columns if deduping.

    Args:
        linker:
            Splink linker object. For more, see splink.linker.

        match_probability_threshold (int):
            The minimum match probability threshold for a link to be
            considered a match. This reduces the number of unique IDs created
            and connected in our algorithm.

    Returns:
        SplinkDataFrame: A dataframe containing two sets of unique IDs,
        unique_id_l and unique_id_r.

    """

    uid_cols = linker._settings_obj._unique_id_input_columns
    uid_concat_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
    uid_concat_edges_r = _composite_unique_id_from_edges_sql(uid_cols, "r")
    uid_concat_edges = _composite_unique_id_from_edges_sql(uid_cols, None)

    # Generate new unique IDs for our linked dataframes.
    sql = f"""
        select
        {uid_concat_edges_l} as unique_id_l,
        {uid_concat_edges_r} as unique_id_r
        from {df_predict.physical_name}
        where match_probability >= {match_probability_threshold}

        UNION

        select
        {uid_concat_edges} as unique_id_l,
        {uid_concat_edges} as unique_id_r
        from __splink__df_concat_with_tf
    """

    return linker._sql_to_splink_dataframe_checking_cache(
        sql, "__splink__df_connected_components_df"
    )


def solve_connected_components(
    linker: "Linker",
    edges_table: SplinkDataFrame,
    _generated_graph=False,
):
    """Connected Components main algorithm.

    This function helps cluster your linked (or deduped) records
    into single groups, which can then be more easily visualised.

    Args:
        linker:
            Splink linker object. For more, see splink.linker.

        edges_table (SplinkDataFrame):
            Splink dataframe containing our edges dataframe to be connected.

        generated_graph (bool):
            Specifies whether the input df is a NetworkX graph, or part of
            a splink deduping or linking job.

            This is used for testing against NetworkX and only impacts how
            our nodes table is generated as this can be shortcut using
            __splink__df_concat_with_tf.

    Returns:
        SplinkDataFrame: A dataframe containing the connected components list
        for your link or dedupe job.

    """

    edges_table = edges_table.physical_name

    # Create our initial node and neighbours tables
    sql = _cc_create_nodes_table(linker, edges_table, _generated_graph)
    linker._enqueue_sql(sql, "nodes")
    sql = _cc_generate_neighbours_representation(edges_table)
    linker._enqueue_sql(sql, "__splink__df_neighbours")
    neighbours = linker._execute_sql_pipeline()

    # Extract our generated neighbours table name.
    # This utilises our caching system to ensure that
    # the problem we are solving is unique to
    # this specific predict() solution and is not solved again.
    neighbours_table = neighbours.physical_name

    # Create our initial representatives table
    sql = _cc_generate_initial_representatives_table(neighbours_table)
    linker._enqueue_sql(sql, "representatives")
    sql = _cc_update_neighbours_first_iter(neighbours_table)
    linker._enqueue_sql(sql, "neighbours_first_iter")
    sql = _cc_update_representatives_first_iter()
    # Execute if we have no batching, otherwise add it to our batched process
    linker._enqueue_sql(sql, "__splink__df_representatives")
    representatives = linker._execute_sql_pipeline()
    prev_representatives_table = representatives

    # Loop while our representative table still has unsettled nodes
    iteration, root_rows = 0, 1
    while root_rows > 0:
        start_time = time.time()
        iteration += 1

        # Loop summary:

        # 1. Update our neighbours table.
        # 2. Join on the representatives table from the previous iteration
        #    to create the "rep_match" column.
        # 3. Assess if our exit condition has been met.

        # Generates our representatives table for the next iteration
        # by joining our previous tables onto our neighbours table.
        sql = _cc_generate_representatives_loop_cond(
            neighbours_table,
            prev_representatives_table.physical_name,
        )
        linker._enqueue_sql(sql, "r")
        # Update our rep_match column in the representatives table.
        sql = _cc_update_representatives_loop_cond(
            prev_representatives_table.physical_name
        )
        representatives = linker._enqueue_sql(
            sql,
            "__splink__df_representatives",
        )

        representatives = linker._execute_sql_pipeline()
        # Update table reference
        prev_representatives_table.drop_table_from_database()
        prev_representatives_table = representatives

        # Check if our exit condition has been met...
        sql = _cc_assess_exit_condition(representatives.physical_name)

        linker._enqueue_sql(sql, "__splink__df_root_rows")

        root_rows_df = linker._execute_sql_pipeline(
            materialise_as_hash=False, use_cache=False
        )

        root_rows = root_rows_df.as_record_dict()
        root_rows_df.drop_table_from_database()
        root_rows = root_rows[0]["count"]
        logger.info(f"Completed iteration {iteration}, root rows count {root_rows}")
        end_time = time.time()
        logger.log(15, f"    Iteration time: {end_time - start_time} seconds")

    # Create our final representatives table
    # Need to edit how we export the table based on whether we are
    # performing a link or dedupe job.

    uid_cols = linker._settings_obj._unique_id_input_columns
    uid_concat = _composite_unique_id_from_nodes_sql(uid_cols, "n")

    exit_query = f"""
        select c.representative as cluster_id, n.*
        from {representatives.physical_name} as c
        left join __splink__df_concat_with_tf as n
        on {uid_concat} = c.node_id

    """

    representatives = linker._sql_to_splink_dataframe_checking_cache(
        exit_query,
        "__splink__df_representatives",
    )

    return representatives

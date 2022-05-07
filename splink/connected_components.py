# This sql code for solving connected components takes inspiration
# from the following paper: https://arxiv.org/pdf/1802.09478.pdf

# While we haven't been able to implement the solution presented
# by the paper - due to SQL backend restrictions with UDFs, -
# we have been able to use the paper to further our understanding
# of the problem and come to a working solution.


def _cc_create_nodes_table(edge_table):

    """
    From our edges table, create a nodes table.

    This captures ALL nodes in our edges representation
    as a single columnar list.
    """

    sql = f"""
    select unique_id_l as node_id
        from {edge_table}

        UNION

    select unique_id_r as node_id
        from {edge_table}
    """

    return sql


def _cc_generate_neighbours_representation(edges_table):

    """
    Using our nodes table, create a representation
    that documents all "neighbours" (any connections
    between nodes) in our graph.
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

    """
    Generate our initial "representatives" table.
    --------------------------------------------

    As outlined in the paper quoted at the top:

    '...begin by choosing for each vertex (node)
    a representatative by picking the vertex with
    the minimum id amongst itself and its neighbours'.

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

    """
    Update our neighbours table - first iteration only.
    ------------------------------------------------

    Takes our initial neighbours table, join on the representatives table
    and recalculates the mimumum representative for each node.

    This works by joining on the current known representative for each node's
    neighbours.

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

    """
    Update our representatives table - first iteration only.
    -----------------------------------------------------

    From here, standardised code can be used inside a while loop,
    as the representatives table no longer needs generating.

    This is only used for the first iteration as we

    In this SQL, we also generate "rep_match", which is a boolean
    that indicates whether the current representative differs
    from the previous representative.

    This value is used extensively to speed up our while loop and as.
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

    """
    ---- Main legs of our while loop ----

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

    """
    Update our representatives table - while loop condition.
    -------------------------------------------------------

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

    """
    Exit condition for our Connected Components algorithm.
    -----------------------------------------------------

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


def solve_connected_components(
    linker,
    edges_table,
    batching,
):

    """
    ---- Connected Components algorithm ----

    This brings together our simpler SQL statements (listed above)
    and generates the final SQL for our algorithm.

    The code will continue to loop until a solution is identified.
    """

    edges_table = edges_table.physical_name

    # Create our initial node and neighbours tables
    sql = _cc_create_nodes_table(edges_table)
    linker._enqueue_sql(sql, "nodes")
    sql = _cc_generate_neighbours_representation(edges_table)
    neighbours = linker._enqueue_and_execute_sql_pipeline(sql, "neighbours")

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
    if batching == 1:
        representatives = linker._enqueue_and_execute_sql_pipeline(
            sql, "representatives"
        )
        representatives_table = representatives.physical_name
    else:
        linker._enqueue_sql(sql, "representatives_init")
        representatives_table = "representatives_init"

    # Loop while our representative table still has unsettled nodes
    iteration, root_rows = 0, 1
    while root_rows > 0:

        iteration += 1

        # Loop summary:

        # 1. Update our neighbours table.
        # 2. Join on the representatives table from the previous iteration
        #  to create the "rep_match" column.
        # 3. Assess if our exit condition has been met.

        # Generates our representatives table for the next iteration
        # by joining our previous tables onto our neighbours table.
        sql = _cc_generate_representatives_loop_cond(
            neighbours_table,
            representatives_table,
        )
        linker._enqueue_sql(sql, "r")
        # Update our rep_match column in the representatives table.
        sql = _cc_update_representatives_loop_cond(representatives_table)
        representatives = linker._enqueue_and_execute_sql_pipeline(
            sql,
            f"representatives_{iteration}",
        )
        # Update table reference
        representatives_table = representatives.physical_name

        # Check if our exit condition has been met...
        sql = _cc_assess_exit_condition(representatives.physical_name)
        dataframe = linker._sql_to_dataframe(
            sql, "__splink__df_root_rows", materialise_as_hash=False
        )
        root_rows = dataframe.as_record_dict()
        dataframe.drop_table_from_database()
        root_rows = root_rows[0]["count"]

    print(f"Exited after {iteration} iterations")
    # Create our final representatives table
    exit_query = f"""
        select node_id, representative
        from {representatives.physical_name}
    """

    representatives = linker._enqueue_and_execute_sql_pipeline(
        exit_query,
        "__splink__df_representatives",
    )

    return representatives

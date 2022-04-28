class ConnectedComponents:
    def __init__(self, linker):
        self.linker = linker

        if self.linker.settings_dict["link_type"] != "dedupe_only":
            return ["Not currently implemented"]

        # find our final model
        predict_dfs = list(
            filter(
                lambda i: i.physical_name.startswith("__splink__df_predict"),
                linker.names_of_tables_created_by_splink,
            )
        )

        # raise error if no predictions have been made
        # -- improve error when finalising class
        if len(predict_dfs) == 0:
            raise Exception(
                "No prediction dataframe found. Please ensure that you ",
                "have run the linker's predict() method before attempting ",
                "to find the model's connected components.",
            )
        else:
            # else grab the most recent prediction
            self.edges = predict_dfs[-1].physical_name
            # add a print statement to clarify that the most recent
            # predict value is being used?

    def connected_components(self):

        # https://arxiv.org/pdf/1802.09478.pdf:
        # 'begin by choosing for each vertex (node) a representatative by
        # picking the vertex with the minimum id amongst itself and its neighbours'

        # i.e. attach neighbours to nodes and find the minumum
        # Note that, since the edges always have the lower id on the left hand side
        # we only need to join on unique_id_r, and pick unique_id_l

        # That is to say, we only bother to find neighbours that are smaller than the node

        # i.e if we want to get the neighbours of node D, we don't need to get both
        # D->C and D->E, we only need to bother getting D->C, because we know in advance
        # this will have a lower minimum that D->E

        # https://arxiv.org/pdf/1802.09478.pdf
        # 'then to improve on that representation by taking the minimum ID amongst
        # the representatives of the vertex and all its neighbours'

        # This time we want:
        # To get all the neighbours of each node
        # For each neighbour, get its representative (the neigbour's representative)
        # For each node_id, to compute its new representative
        # use the minimum of the neighbours' representatives

        # Note that in this case, we want to get all neighbours of each node
        # IN BOTH DIRECTIONS (hence the union)

        # i.e. we want to retreieve all neighbours
        # EITHER SMALLER OR GREATER, and get THEIR representative

        # For example, supposing we have an edge C -> D.  It may be the case that the
        # representative of D is A.  So we want to retreieve C->D even though D is greater
        # than C, because D's representative may be less than C.

        # >> Get all the neighbours of C,

        # >> This time join on not the edges, but the result of the above query

        # Generate a table that records ALL neighbours for each node -
        # this includes circular connections (i.e. A->A), as well as
        # current connections and the reverse of these connections
        # (i.e. A->B and B->A would both be included in our representation).

        # Circular connections are only included where our reverse
        # connection is null when we apply our joins.
        neighbours_table = f"""

            with nodes as (
                select unique_id_l as node_id
                from {self.edges}
                UNION
                select unique_id_r as node_id
                from {self.edges}
            )

            select n.node_id,
                e_l.unique_id_r as neighbour
            from nodes as n

            left join {self.edges} as e_l
                on n.node_id = e_l.unique_id_l


            UNION ALL


            select n.node_id,
                coalesce(e_r.unique_id_l, n.node_id) as neighbour
            from nodes as n

            left join {self.edges} as e_r
                on n.node_id = e_r.unique_id_r

        """

        # create our neighbours table
        self.linker.enqueue_sql(neighbours_table, "neighbours")
        self.linker.execute_sql_pipeline([], use_cache=False, materialise_as_hash=False)

        # loop while our representative table still has nodes
        # not connected to a "root node"
        iteration, root_rows, repr_rows = 0, -1, 1

        while root_rows != repr_rows:

            iteration += 1
            representatives_name = f"representatives_{iteration}"

            if iteration == 1:
                # if this is the first iteration, we just need to create
                # our initial representatives table
                sql = """
                    select
                        neighbours.node_id,
                        min(neighbour) as representative

                    from neighbours

                    group by node_id
                    order by node_id
                """
                self.linker.enqueue_sql(sql, representatives_name)
                representatives = self.linker.execute_sql_pipeline(
                    [], use_cache=False, materialise_as_hash=False
                )

                # generate count of representative rows that we can compare
                # our exit condition against (see root_nodes)
                representative_count = f"""
                    select count(*) as count
                    from {representatives_name}
                """

                representatives_table = "representatives"
                dataframe = self.linker.sql_to_dataframe(
                    representative_count, "__splink__df_representatives_table_count"
                )
                repr_rows = dataframe.as_record_dict()
                dataframe.drop_table_from_database()
                repr_rows = repr_rows[0]["count"]

            else:

                sql = f"""

                select
                    neighbours.node_id,
                    min(representatives.representative) as representative

                from neighbours

                left join {representatives_table} as representatives

                on neighbours.neighbour = representatives.node_id

                group by neighbours.node_id

                """

                self.linker.enqueue_sql(sql, representatives_name)
                representatives = self.linker.execute_sql_pipeline(
                    [], use_cache=False, materialise_as_hash=False
                )

            # update representative table name
            representatives_table = representatives_name

            # Finally, update and evaluate our representative table
            # to see if all nodes are connected to a "root node".

            # If True, terminate the loop.
            sql = f"""

            with root_nodes as (

                select
                    node_id,
                    representative,
                    node_id=representative as root_node

                from {representatives_name}
            )

            select count(root_nodes.root_node) as count

            from root_nodes

            right join {representatives_name} as representatives

            on root_nodes.node_id = representatives.representative

            where root_nodes.root_node = true

            """
            dataframe = self.linker.sql_to_dataframe(
                sql, "__splink__df_root_rows", materialise_as_hash=False
            )
            root_rows = dataframe.as_record_dict()
            dataframe.drop_table_from_database()
            root_rows = root_rows[0]["count"]

        print(f"Exited after {iteration} iterations")

        return representatives

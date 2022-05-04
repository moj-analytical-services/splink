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

        # That is to say, we only bother to find neighbours that are
        # smaller than the node

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
        # representative of D is A.  So we want to retreieve C->D even though D
        # is greater than C, because D's representative may be less than C.

        # >> Get all the neighbours of C,

        # >> This time join on not the edges, but the result of the above query

        # Generate a table that records ALL neighbours for each node -
        # this includes circular connections (i.e. A->A), as well as
        # current connections and the reverse of these connections
        # (i.e. A->B and B->A would both be included in our representation).

        # Circular connections are only included where our reverse
        # connection is null when we apply our joins.

        sql = f"""
        select unique_id_l as node_id
            from {self.edges}

            UNION

        select unique_id_r as node_id
            from {self.edges}
        """

        self.linker.enqueue_sql(sql, "nodes")

        neighbours_table = f"""
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

        # queue and create our neighbours table
        neighbours_table_name = "__splink__df_neighbours"
        self.linker.enqueue_sql(neighbours_table, neighbours_table_name)
        self.linker.execute_sql_pipeline([], use_cache=False, materialise_as_hash=False)

        # Create our initial representatives table
        # this can be added to our pipeline code later!
        sql = f"""
            select
                neighbours.node_id,
                min(neighbour) as representative
            from {neighbours_table_name} as neighbours
            group by node_id
            order by node_id
        """

        self.linker.enqueue_sql(sql, "representatives")

        sql = f"""
            select
                neighbours.node_id,
                min(representatives.representative) as representative
            from {neighbours_table_name} as neighbours
            left join representatives
            on neighbours.neighbour = representatives.node_id
                group by neighbours.node_id
                order by neighbours.node_id
        """

        self.linker.enqueue_sql(sql, "current_repr")

        sql = """
            select
                current_repr.node_id,
                current_repr.representative,
                current_repr.representative <> repr.representative as rep_match
            from current_repr
            left join representatives as repr
            on current_repr.node_id = repr.node_id
        """

        # create our initial representatives table
        representatives_name = "representatives"
        self.linker.enqueue_sql(sql, representatives_name)
        self.linker.execute_sql_pipeline([], use_cache=False, materialise_as_hash=False)

        # loop while our representative table still has unsettled nodes
        iteration, root_rows = 0, 1

        while root_rows > 0:

            iteration += 1

            """
            Code summary:

            The "r" table is the "representatives" table:
            This utilises our neighbours table and our known representatives.

            The idea is that we join on the representative for all
            known neighbours of a specific node. So if we know that
            (A->B and B->C), then on a subsequent iteration we can
            infer that (A->C) by joining on B.

            To speed up the code, we use a breakdown named "rep_match",
            which takes a boolean value and indicates whether there a
            match on the current and previous representative for the node.

            This tells us if the node has reached its final
            representative, by checking to see if any links have taken
            place. By filtering on this, we can skip nodes that have
            already been fully connected to a "base node".

            The final select statement creates this "rep_match" by
            joining on the previous representative of the node and
            comparing it to the current representative.

            This "rep_match" also in turn becomes our exit condition
            for our "while" loop.

            In short, where every node's representative has stopped changing,
            we can exit the loop.
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

                    from {neighbours_table_name} as neighbours

                    left join {representatives_name} as repr_neighbour
                    on neighbours.neighbour = repr_neighbour.node_id

                    where
                        repr_neighbour.rep_match

                    UNION ALL

                    select

                        node_id,
                        representative

                    from {representatives_name}

                )
                group by node_id
            """

            self.linker.enqueue_sql(sql, "r")

            sql = f"""
                select

                    r.node_id,
                    r.representative,
                    r.representative <> repr.representative as rep_match

                from r

                left join {representatives_name} as repr
                on r.node_id = repr.node_id
            """
            # set new representatives name (this will be used in the next iteration)
            representatives_name = f"representatives_{iteration}"

            self.linker.enqueue_sql(sql, representatives_name)
            self.linker.execute_sql_pipeline(
                [], use_cache=False, materialise_as_hash=False
            )

            # Finally, evaluate our representatives table
            # to see if all nodes have a constant representative
            # (indicated by rep_match).

            # If True, terminate the loop.
            sql = f"""
                select count(*) as count
                from {representatives_name}
                where rep_match
            """
            dataframe = self.linker.sql_to_dataframe(
                sql, "__splink__df_root_rows", materialise_as_hash=False
            )
            root_rows = dataframe.as_record_dict()
            dataframe.drop_table_from_database()
            root_rows = root_rows[0]["count"]

        print(f"Exited after {iteration} iterations")
        exit_query = f"""
            select node_id, representative
            from {representatives_name}
        """
        self.linker.enqueue_sql(exit_query, "__splink__df_connected_components")
        representatives = self.linker.execute_sql_pipeline(
            [], use_cache=False, materialise_as_hash=False
        )

        return representatives

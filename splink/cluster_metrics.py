from splink.input_column import InputColumn


def _size_density_sql(
    df_predict, df_clustered, threshold_match_probability, _unique_id_col
):
    """Generates sql for computing cluster size and density at a given threshold.

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`
        df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
        threshold_match_probability (float): Filter the pairwise match
            predictions to include only pairwise comparisons with a
            match_probability above this threshold.
        _unique_id_col (string): name of unique id column in settings dict

    Returns:
        sql string for computing cluster size and density
    """

    # Get physical table names from Splink dataframes
    edges_table = df_predict.physical_name
    clusters_table = df_clustered.physical_name

    input_col = InputColumn(_unique_id_col)
    unique_id_col_l = input_col.name_l

    sqls = []
    sql = f"""
        SELECT
            {unique_id_col_l},
            COUNT(*) AS count_edges
        FROM {edges_table}
        WHERE match_probability >= {threshold_match_probability}
        GROUP BY {unique_id_col_l}
    """

    sql = {"sql": sql, "output_table_name": "__splink__count_edges"}
    sqls.append(sql)

    sql = f"""
        SELECT
            c.cluster_id,
            count(*) AS n_nodes,
            sum(e.count_edges) AS n_edges
        FROM {clusters_table} AS c
        LEFT JOIN __splink__count_edges e ON c.{_unique_id_col} = e.{unique_id_col_l}
        GROUP BY c.cluster_id
    """
    sql = {"sql": sql, "output_table_name": "__splink__counts_per_cluster"}
    sqls.append(sql)

    sql = """
        SELECT
            cluster_id,
            n_nodes,
            n_edges,
            (n_edges * 2)/(n_nodes * (n_nodes-1)) AS density
        FROM __splink__counts_per_cluster
    """
    sql = {"sql": sql, "output_table_name": "__splink__cluster_metrics_clusters"}
    sqls.append(sql)

    return sqls

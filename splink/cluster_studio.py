from typing import TYPE_CHECKING
from .splink_dataframe import SplinkDataFrame
from .unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def _quo_if_str(x):
    if type(x) is str:
        return f"'{x}'"
    else:
        return str(x)


def _clusters_sql(df_clustered_nodes, cluster_ids: list) -> str:

    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select distinct(cluster_id)
    from {df_clustered_nodes.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def df_clusters_as_records(
    linker: "Linker", df_clustered_nodes: SplinkDataFrame, cluster_ids: list
):
    sql = _clusters_sql(df_clustered_nodes, cluster_ids)
    df_clusters = linker._enqueue_and_execute_sql_pipeline(
        sql, "__splink__scs_clusters"
    )
    return df_clusters.as_record_dict()


def _nodes_sql(df_clustered_nodes, cluster_ids) -> str:

    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select *
    from {df_clustered_nodes.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def create_df_nodes(
    linker: "Linker", df_clustered_nodes: SplinkDataFrame, cluster_ids: list
):
    sql = _nodes_sql(df_clustered_nodes, cluster_ids)
    df_nodes = linker._enqueue_and_execute_sql_pipeline(sql, "__splink__scs_clusters")
    return df_nodes


def _edges_sql(
    linker: "Linker", df_predicted_edges: SplinkDataFrame, df_nodes: SplinkDataFrame
):

    unique_id_cols = linker._settings_obj._unique_id_input_columns

    nodes_l_id_expr = _composite_unique_id_from_nodes_sql(unique_id_cols, "nodes_l")
    nodes_r_id_expr = _composite_unique_id_from_nodes_sql(unique_id_cols, "nodes_r")
    edges_l_id_expr = _composite_unique_id_from_edges_sql(
        unique_id_cols, "l", table_prefix="edges"
    )
    edges_r_id_expr = _composite_unique_id_from_edges_sql(
        unique_id_cols, "r", table_prefix="edges"
    )

    # It's in the cluster if BOTH the left and right id are in the cluster
    # So it's a double left join you need

    sql = f"""
    select
        edges.*,
        nodes_l.cluster_id as cluster_id_l,
        nodes_r.cluster_id as cluster_id_r
    from {df_predicted_edges.physical_name} as edges
    inner join
    {df_nodes.physical_name} as nodes_l
    on {edges_l_id_expr} = {nodes_l_id_expr}
    inner join
    {df_nodes.physical_name} as nodes_r
    on {edges_r_id_expr} = {nodes_r_id_expr}
    """
    return sql


def df_edges_as_records(
    linker: "Linker", df_predicted_edges: SplinkDataFrame, df_nodes: SplinkDataFrame
):
    sql = _edges_sql(linker, df_predicted_edges, df_nodes)
    df_edges = linker._enqueue_and_execute_sql_pipeline(sql, "__splink__scs_edges")
    return df_edges.as_record_dict()


def render_splink_cluster_studio_html(
    linker: "Linker",
    df_predicted_edges: SplinkDataFrame,
    df_clustered_nodes: SplinkDataFrame,
    cluster_ids,
):

    cluster_recs = df_clusters_as_records(linker, df_clustered_nodes, cluster_ids)
    df_nodes = create_df_nodes(linker, df_clustered_nodes, cluster_ids)
    nodes_recs = df_nodes.as_record_dict()
    edges_recs = df_edges_as_records(linker, df_predicted_edges, df_nodes)

    # Render template with cluster, nodes and edges

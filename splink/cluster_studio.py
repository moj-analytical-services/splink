from typing import TYPE_CHECKING
from .splink_dataframe import SplinkDataFrame

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def _quo_if_str(x):
    if type(x) is str:
        return f"'{x}'"
    else:
        return str(x)


def _clusters_sql(df_clustered, cluster_ids: list) -> str:

    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select distinct(cluster_id)
    from {df_clustered.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def df_clusters_as_records(
    linker: "Linker", df_clustered: SplinkDataFrame, cluster_ids: list
):
    sql = _clusters_sql(df_clustered, cluster_ids)
    df_clusters = linker._enqueue_and_execute_sql_pipeline(
        sql, "__splink__scs_clusters"
    )
    return df_clusters.as_record_dict()


def _nodes_sql(df_clustered, cluster_ids) -> str:

    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select *
    from {df_clustered.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def _edges_sql(df_predict: SplinkDataFrame, cluster_ids: list):

    # It's in the cluster if BOTH the left and right id are in the cluster
    # So it's a double left join you need

    sql = f"""
    select *
    from {df_predict.physical_name}
    where cluster_id in ({cluster_ids})
    """


def df_nodes_as_records(
    linker: "Linker", df_clustered: SplinkDataFrame, cluster_ids: list
):
    sql = _nodes_sql(df_clustered, cluster_ids)
    df_nodes = linker._enqueue_and_execute_sql_pipeline(sql, "__splink__scs_clusters")
    return df_nodes.as_record_dict()


def render_splink_cluster_studio_html(
    linker: "Linker",
    df_predict: SplinkDataFrame,
    df_clustered: SplinkDataFrame,
    cluster_ids,
):

    cluster_recs = df_clusters_as_records(linker, df_clustered, cluster_ids)
    node_recs = df_nodes_as_records(linker, df_clustered, cluster_ids)
    return node_recs

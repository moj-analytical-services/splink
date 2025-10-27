from __future__ import annotations

import json
import os
import random
from typing import TYPE_CHECKING, Any, Literal, Optional

from splink.internals.exceptions import SplinkException
from splink.internals.misc import EverythingEncoder, read_resource
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.linker import Linker

SamplingMethods = Literal[
    "random", "by_cluster_size", "lowest_density_clusters_by_size"
]


def _quo_if_str(x):
    if isinstance(x, str):
        return f"'{x}'"
    else:
        return str(x)


def _clusters_sql(df_clustered_nodes: SplinkDataFrame, cluster_ids: list[str]) -> str:
    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select distinct(cluster_id)
    from {df_clustered_nodes.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def df_clusters_as_records(
    linker: "Linker", df_clustered_nodes: SplinkDataFrame, cluster_ids: list[str]
) -> list[dict[str, Any]]:
    """Retrieves distinct clusters which exist in df_clustered_nodes based on
    list of cluster IDs provided and converts them to a record dictionary.

    Args:
        linker: An instance of the Splink Linker class.
        df_clustered_nodes (SplinkDataFrame): Result of
        cluster_pairwise_predictions_at_threshold().
        cluster_ids (list): List of cluster IDs to filter the results.

    Returns:
    dict: A record dictionary of the specified cluster IDs.
    """
    sql = _clusters_sql(df_clustered_nodes, cluster_ids)
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__scs_clusters")
    df_clusters = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_clusters.as_record_dict()


def _nodes_sql(df_clustered_nodes: SplinkDataFrame, cluster_ids: list[str]) -> str:
    """Generates SQL query to select all columns from df_clustered_nodes
    for list of cluster IDs provided.

    Args:
        df_clustered_nodes (SplinkDataFrame): result of
        cluster_pairwise_predictions_at_threshold()
        cluster_ids (list): List of cluster IDs to filter the results
    """

    cluster_ids = [_quo_if_str(x) for x in cluster_ids]
    cluster_ids_joined = ", ".join(cluster_ids)

    sql = f"""
    select *
    from {df_clustered_nodes.physical_name}
    where cluster_id in ({cluster_ids_joined})
    """

    return sql


def create_df_nodes(
    linker: "Linker", df_clustered_nodes: SplinkDataFrame, cluster_ids: list[str]
) -> SplinkDataFrame:
    """Retrieves nodes from df_clustered_nodes for list of cluster IDs provided.

    Args:
        linker: An instance of the Splink Linker class.
        df_clustered_nodes (SplinkDataFrame): Result of
        cluster_pairwise_predictions_at_threshold().
        cluster_ids (list): List of cluster IDs to filter the results.

    Returns:
        A SplinkDataFrame containing the nodes for the specified cluster IDs.
    """
    pipeline = CTEPipeline()
    sql = _nodes_sql(df_clustered_nodes, cluster_ids)
    pipeline.enqueue_sql(sql, "__splink__scs_nodes")
    df_nodes = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_nodes


def _edges_sql(
    linker: "Linker", df_predicted_edges: SplinkDataFrame, df_nodes: SplinkDataFrame
) -> str:
    unique_id_cols = linker._settings_obj.column_info_settings.unique_id_input_columns

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
) -> list[dict[str, Any]]:
    sql = _edges_sql(linker, df_predicted_edges, df_nodes)
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__scs_edges")
    df_edges = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return df_edges.as_record_dict()


def _get_random_cluster_ids(
    linker: "Linker",
    connected_components: SplinkDataFrame,
    sample_size: int,
    seed: int | None = None,
) -> list[str]:
    sql = f"""
    select count(distinct cluster_id) as count
    from {connected_components.physical_name}
    """
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__cluster_count")
    df_cluster_count = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    cluster_count = df_cluster_count.as_record_dict()[0]["count"]
    df_cluster_count.drop_table_from_database_and_remove_from_cache()

    proportion = sample_size / cluster_count

    random_sample_sql = linker._random_sample_sql(
        proportion,
        sample_size,
        seed,
        table=connected_components.physical_name,
        unique_id="cluster_id",
    )

    sql = f"""
    with distinct_clusters as (
    select distinct(cluster_id)
    from {connected_components.physical_name}
    )
    select cluster_id from distinct_clusters
    {random_sample_sql}
    """
    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql, "__splink__df_concat_with_tf_sample")
    df_sample = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    return [r["cluster_id"] for r in df_sample.as_record_dict()]


def _get_cluster_id_of_each_size(
    linker: "Linker", connected_components: SplinkDataFrame, rows_per_partition: int
) -> list[dict[str, Any]]:
    unique_id_col_name = linker._settings_obj.column_info_settings.unique_id_column_name
    pipeline = CTEPipeline()
    sql = f"""
    select
        cluster_id,
        count(*) as cluster_size,
        max({unique_id_col_name}) as ordering
    from {connected_components.physical_name}
    group by cluster_id
    having count(*)>1
    """

    pipeline.enqueue_sql(sql, "__splink__cluster_count")

    # Assign unique row number to each row in partition
    sql = """
    select
        cluster_id,
        cluster_size,
        row_number() over (partition by cluster_size order by ordering) as row_num
    from __splink__cluster_count
    """

    pipeline.enqueue_sql(sql, "__splink__cluster_count_row_numbered")

    sql = f"""
    select
        cluster_id,
        cluster_size
    from __splink__cluster_count_row_numbered
    where row_num <= {rows_per_partition}
    """

    pipeline.enqueue_sql(sql, "__splink__cluster_count_row_numbered_2")
    df_cluster_sample_with_size = linker._db_api.sql_pipeline_to_splink_dataframe(
        pipeline
    )

    return df_cluster_sample_with_size.as_record_dict()


def _get_lowest_density_clusters(
    linker: "Linker",
    df_cluster_metrics: SplinkDataFrame,
    rows_per_partition: int,
    min_nodes: int,
) -> list[dict[str, Any]]:
    """Returns lowest density clusters of different sizes by
    performing stratified sampling.

    Args:
        linker: An instance of the Splink Linker class.
        df_cluster_metrics (SplinkDataFrame): dataframe containing
        cluster metrics including density.
        rows_per_partition (int): number of rows in each strata (partition)
        min_nodes (int): minimum number of nodes a cluster must contain
        to be included in the sample.

    Returns:
        list: A list of record dictionaries containing cluster ids, densities
        and sizes of lowest density clusters.
    """
    pipeline = CTEPipeline()
    sql = f"""
    select
        cluster_id,
        n_nodes,
        density,
        row_number() over (partition by n_nodes order by density, cluster_id) as row_num
    from {df_cluster_metrics.physical_name}
    where n_nodes >= {min_nodes}
    """

    pipeline.enqueue_sql(sql, "__splink__partition_clusters_by_size")

    sql = f"""
    select
        cluster_id,
        round(density, 4) as density_4dp,
        n_nodes as cluster_size
    from __splink__partition_clusters_by_size
    where row_num <= {rows_per_partition}
    """

    pipeline.enqueue_sql(sql, "__splink__lowest_density_clusters")
    df_lowest_density_clusters = linker._db_api.sql_pipeline_to_splink_dataframe(
        pipeline
    )

    return df_lowest_density_clusters.as_record_dict()


def _get_cluster_ids(
    linker: "Linker",
    df_clustered_nodes: SplinkDataFrame,
    sampling_method: SamplingMethods,
    sample_size: int,
    sample_seed: int | None,
    _df_cluster_metrics: Optional[SplinkDataFrame] = None,
) -> tuple[list[str], list[str]]:
    if sampling_method == "random":
        cluster_ids = _get_random_cluster_ids(
            linker, df_clustered_nodes, sample_size, sample_seed
        )
        cluster_names = []
    elif sampling_method == "by_cluster_size":
        cluster_id_infos = _get_cluster_id_of_each_size(
            linker, df_clustered_nodes, rows_per_partition=1
        )
        if len(cluster_id_infos) > sample_size:
            cluster_id_infos = random.sample(cluster_id_infos, k=sample_size)
        cluster_names = [
            f"Cluster ID: {c['cluster_id']}, size:  {c['cluster_size']}"
            for c in cluster_id_infos
        ]
        cluster_ids = [c["cluster_id"] for c in cluster_id_infos]
    elif sampling_method == "lowest_density_clusters_by_size":
        if _df_cluster_metrics is None:
            raise SplinkException(
                """To sample by density, you must provide a cluster metrics table
                    containing density. This can be generated by calling the
                    _compute_graph_metrics method on the linker."""
            )
        # Using sensible default for min_nodes. Might become option
        # for users in future
        cluster_id_infos = _get_lowest_density_clusters(
            linker, _df_cluster_metrics, rows_per_partition=1, min_nodes=3
        )
        if len(cluster_id_infos) > sample_size:
            cluster_id_infos = random.sample(cluster_id_infos, k=sample_size)
        cluster_names = [
            f"""Cluster ID: {c['cluster_id']}, density (4dp): {c['density_4dp']},
            size: {c['cluster_size']}"""
            for c in cluster_id_infos
        ]
        cluster_ids = [c["cluster_id"] for c in cluster_id_infos]
    else:
        raise ValueError(f"Unknown sampling method {sampling_method}")
    return cluster_ids, cluster_names


def render_splink_cluster_studio_html(
    linker: "Linker",
    df_predicted_edges: SplinkDataFrame,
    df_clustered_nodes: SplinkDataFrame,
    out_path: str,
    sampling_method: SamplingMethods = "random",
    sample_size: int = 10,
    sample_seed: int | None = None,
    cluster_ids: list[str] = None,
    cluster_names: list[str] = None,
    overwrite: bool = False,
    _df_cluster_metrics: SplinkDataFrame = None,
) -> str:
    from jinja2 import Template

    bundle_observable_notebook = True

    svu_options = {
        "cluster_colname": "cluster_id",
        "prob_colname": "match_probability",
    }
    if cluster_ids is None:
        cluster_ids, cluster_names = _get_cluster_ids(
            linker,
            df_clustered_nodes,
            sampling_method,
            sample_size,
            sample_seed,
            _df_cluster_metrics,
        )

    cluster_recs = df_clusters_as_records(linker, df_clustered_nodes, cluster_ids)
    df_nodes = create_df_nodes(linker, df_clustered_nodes, cluster_ids)
    nodes_recs = df_nodes.as_record_dict()
    edges_recs = df_edges_as_records(linker, df_predicted_edges, df_nodes)

    # Render template with cluster, nodes and edges
    template_path = "internals/files/splink_cluster_studio/cluster_template.j2"
    template = Template(read_resource(template_path))

    template_data: dict[str, Any] = {
        "raw_edge_data": json.dumps(edges_recs, cls=EverythingEncoder),
        "raw_node_data": json.dumps(nodes_recs, cls=EverythingEncoder),
        "raw_clusters_data": json.dumps(cluster_recs, cls=EverythingEncoder),
        "splink_settings": json.dumps(
            linker._settings_obj._as_completed_dict(), cls=EverythingEncoder
        ),
        "svu_options": json.dumps(svu_options, cls=EverythingEncoder),
    }

    if cluster_names:
        named_clusters_dict = dict(zip(cluster_ids, cluster_names))

        template_data["named_clusters"] = json.dumps(
            named_clusters_dict, cls=EverythingEncoder
        )

    files = {
        "embed": "internals/files/external_js/vega-embed@6.20.2",
        "stdlib": "internals/files/external_js/stdlib.js@5.8.3",
        "vega": "internals/files/external_js/vega@5.31.0",
        "vegalite": "internals/files/external_js/vega-lite@5.2.0",
        "svu_text": "internals/files/splink_vis_utils/splink_vis_utils.js",
        "custom_css": "internals/files/splink_cluster_studio/custom.css",
    }
    for k, v in files.items():
        template_data[k] = read_resource(v)

    template_data["bundle_observable_notebook"] = bundle_observable_notebook

    rendered = template.render(**template_data)

    if os.path.isfile(out_path) and not overwrite:
        raise ValueError(
            f"The path {out_path} already exists. Please provide a different path."
        )
    else:
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            dbutils.fs.put(out_path, rendered, overwrite=True)
            # to view the dashboard inline in notebook displayHTML(rendered)
            return rendered
        else:
            with open(out_path, "w", encoding="utf-8") as html_file:
                html_file.write(rendered)
            # return the rendered dashboard html for inline viewing in the notebook
            return rendered

import logging

from splink.internals.cluster_studio import (
    _get_lowest_density_clusters,
    _get_random_cluster_ids,
)
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def _make_linker_with_clusters(cluster_ids):
    person_ids = [i + 1 for i in range(len(cluster_ids))]
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "person_id",
    }
    db_api = DuckDBAPI()
    df_sdf = db_api.register({"person_id": person_ids})
    linker = Linker(df_sdf, settings)

    df_clustered_nodes = linker.table_management.register_table(
        {"person_id": person_ids, "cluster_id": cluster_ids},
        "df_clustered_nodes",
        overwrite=True,
    )
    return linker, df_clustered_nodes


def test_random_sample_size_exceeds_cluster_count_warns(caplog):
    # Only 3 distinct clusters exist
    cluster_ids = ["A", "A", "B", "B", "C"]
    linker, df_clustered_nodes = _make_linker_with_clusters(cluster_ids)

    with caplog.at_level(logging.WARNING):
        result = _get_random_cluster_ids(linker, df_clustered_nodes, sample_size=10)

    assert sorted(result) == ["A", "B", "C"]
    assert any(
        "requested sample_size (10)" in record.message and "3" in record.message
        for record in caplog.records
    )


def test_random_sample_size_within_cluster_count_no_warning(caplog):
    cluster_ids = ["A", "A", "B", "B", "C"]
    linker, df_clustered_nodes = _make_linker_with_clusters(cluster_ids)

    with caplog.at_level(logging.WARNING):
        result = _get_random_cluster_ids(linker, df_clustered_nodes, sample_size=2)

    assert len(result) == 2
    assert not any(
        "requested sample_size" in record.message for record in caplog.records
    )


def test_density_sample():
    # Simple df and settings for linker
    person_ids = [i + 1 for i in range(5)]

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "person_id",
    }
    db_api = DuckDBAPI()
    df_sdf = db_api.register({"person_id": person_ids})
    linker = Linker(df_sdf, settings)

    # Dummy cluster metrics table
    cluster = ["A", "B", "C", "D", "E", "F"]
    n_nodes = [2, 3, 3, 3, 10, 10]
    n_edges = [1, 2, 2, 3, 9, 20]
    density = [
        (n_edges * 2) / (n_nodes * (n_nodes - 1))
        for n_nodes, n_edges in zip(n_nodes, n_edges)
    ]
    pd_metrics = {
        "cluster_id": cluster,
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "density": density,
    }

    # Convert to Splink dataframe
    df_cluster_metrics = linker.table_management.register_table(
        pd_metrics, "df_cluster_metrics", overwrite=True
    )
    result = _get_lowest_density_clusters(
        linker, df_cluster_metrics, rows_per_partition=1, min_nodes=3
    )

    result = sorted(result, key=lambda x: x["cluster_id"])

    expect = [
        {"cluster_id": "B", "density_4dp": 0.6667, "cluster_size": 3},
        {"cluster_id": "E", "density_4dp": 0.2, "cluster_size": 10},
    ]

    assert result == expect

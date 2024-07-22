import pandas as pd

from splink.internals.cluster_studio import _get_lowest_density_clusters
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def test_density_sample():
    # Simple df and settings for linker
    person_ids = [i + 1 for i in range(5)]
    df = pd.DataFrame({"person_id": person_ids})

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "person_id",
    }
    linker = Linker(df, settings, db_api=DuckDBAPI())

    # Dummy cluster metrics table
    cluster = ["A", "B", "C", "D", "E", "F"]
    n_nodes = [2, 3, 3, 3, 10, 10]
    n_edges = [1, 2, 2, 3, 9, 20]
    density = [
        (n_edges * 2) / (n_nodes * (n_nodes - 1))
        for n_nodes, n_edges in zip(n_nodes, n_edges)
    ]
    pd_metrics = pd.DataFrame(
        {
            "cluster_id": cluster,
            "n_nodes": n_nodes,
            "n_edges": n_edges,
            "density": density,
        }
    )

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

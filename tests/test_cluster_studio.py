import pandas as pd

from splink.cluster_studio import _get_lowest_density_cluster_ids
from splink.duckdb.linker import DuckDBLinker


def test_density_sample():
    # Simple df and settings for linker
    person_ids = [i + 1 for i in range(5)]
    df = pd.DataFrame({"person_id": person_ids})

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "person_id",
    }
    linker = DuckDBLinker(df, settings)

    # Dummy cluster metrics table
    cluster = ["A", "B", "C", "D", "E"]
    n_nodes = [3, 2, 10, 3, 19]
    n_edges = [2, 1, 5, 2, 25]
    density = [
        (n_edges * 2) / (n_nodes * (n_nodes - 1))
        for n_nodes, n_edges in zip(n_nodes, n_edges)
    ]
    df_metrics = pd.DataFrame(
        {
            "cluster_id": cluster,
            "n_nodes": n_nodes,
            "n_edges": n_edges,
            "density": density,
        }
    )

    # Convert to Splink dataframe
    df_cluster_metrics = linker.register_table(
        df_metrics, "df_cluster_metrics", overwrite=True
    )
    df_result = _get_cluster_id_by_density(
        linker, df_cluster_metrics, sample_size=3, min_nodes=3
    )
    df_expect = ["C", "E", "A"]
    assert df_result == df_expect

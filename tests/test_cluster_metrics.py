import pandas as pd
from pandas.testing import assert_frame_equal

from splink.duckdb.linker import DuckDBLinker

# Dummy df
person_ids = [i + 1 for i in range(5)]
df = pd.DataFrame({"person_id": person_ids})

# Dummy edges df
edges_data = [
    # cluster A edges
    {"person_id_l": 1, "person_id_r": 2, "match_probability": 0.99},
    {"person_id_l": 1, "person_id_r": 3, "match_probability": 0.99},
    # cluster B edge
    {"person_id_l": 4, "person_id_r": 5, "match_probability": 0.99},
    # edges not in relevant clusters
    {"person_id_l": 10, "person_id_r": 11, "match_probability": 0.99},
    {"person_id_l": 12, "person_id_r": 12, "match_probability": 0.95},
]
edges = pd.DataFrame(edges_data)

# Dummy clusters df
cluster_ids = ["A", "A", "A", "B", "B"]
clusters_data = {"cluster_id": cluster_ids, "person_id": person_ids}
clusters = pd.DataFrame(clusters_data)

# Expected dataframe
expected_data = [
    {"cluster_id": "A", "n_nodes": 3, "n_edges": 2.0, "density": 2 / 3},
    {"cluster_id": "B", "n_nodes": 2, "n_edges": 1.0, "density": 1.0},
]
df_expected = pd.DataFrame(expected_data)


def test_size_density():
    # Linker with basic settings
    settings = {"link_type": "dedupe_only", "unique_id_column_name": "person_id"}
    linker = DuckDBLinker(df, settings)

    # Register as Splink dataframes
    df_predict = linker.register_table(edges, "df_predict", overwrite=True)
    df_clustered = linker.register_table(clusters, "df_clustered", overwrite=True)

    df_cluster_metrics = linker._compute_cluster_metrics(
        df_predict, df_clustered, threshold_match_probability=0.99
    )
    df_cluster_metrics = df_cluster_metrics.as_pandas_dataframe()

    assert_frame_equal(df_cluster_metrics, df_expected)

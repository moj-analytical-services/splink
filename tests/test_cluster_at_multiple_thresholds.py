import numpy as np
import pandas as pd
import pytest

from splink.internals.clustering import (
    cluster_pairwise_predictions_at_multiple_thresholds,
    cluster_pairwise_predictions_at_threshold,
)
from tests.cc_testing_utils import generate_random_graph, nodes_and_edges_from_graph

from .decorator import mark_with_dialects_excluding


@pytest.mark.parametrize("graph_size", [100, 500, 1000, 5000])
@mark_with_dialects_excluding()
def test_cluster_at_multiple_thresholds(test_helpers, dialect, graph_size):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    if dialect == "spark" and graph_size > 20:
        pytest.skip("Skipping large graph sizes for Spark dialect")

    G = generate_random_graph(graph_size)
    combined_nodes, combined_edges = nodes_and_edges_from_graph(G)

    combined_edges["match_probability"] = np.random.uniform(0, 1, len(combined_edges))

    thresholds = [0.5, 0.7]

    all_clusters = cluster_pairwise_predictions_at_multiple_thresholds(
        combined_nodes,
        combined_edges,
        node_id_column_name="unique_id",
        db_api=db_api,
        match_probability_thresholds=thresholds,
    )

    all_clusters_pd = all_clusters.as_pandas_dataframe()

    cluster_cols = [c for c in all_clusters_pd.columns if "cluster" in c]

    for threshold, cluster_col in zip(thresholds, cluster_cols):
        single_threshold_clusters = cluster_pairwise_predictions_at_threshold(
            combined_nodes,
            combined_edges,
            node_id_column_name="unique_id",
            db_api=db_api,
            threshold_match_probability=threshold,
        )

        single_threshold_clusters_pd = single_threshold_clusters.as_pandas_dataframe()

        multi_threshold_result = all_clusters_pd[["unique_id", cluster_col]]

        single_threshold_result = single_threshold_clusters_pd[
            ["unique_id", "cluster_id"]
        ]

        multi_threshold_result = multi_threshold_result.sort_values(
            "unique_id"
        ).reset_index(drop=True)
        single_threshold_result = single_threshold_result.sort_values(
            "unique_id"
        ).reset_index(drop=True)

        multi_threshold_result.columns = ["unique_id", "cluster_id"]

        pd.testing.assert_frame_equal(multi_threshold_result, single_threshold_result)

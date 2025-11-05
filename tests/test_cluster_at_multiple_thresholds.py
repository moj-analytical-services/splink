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

    if dialect in {"postgres", "sqlite"} and graph_size > 500:
        pytest.skip("Skipping large graph sizes for Postgres/SQLite dialects")

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


# This is slow in Spark, and so long as this passes in duckdb, there's no reason it
# shouldn't in Spark
@mark_with_dialects_excluding("spark")
def test_cluster_at_multiple_thresholds_mw_prob_equivalence(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.DatabaseAPI(**helper.db_api_args())

    nodes = [
        {"my_id": 1},
        {"my_id": 2},
        {"my_id": 3},
        {"my_id": 4},
        {"my_id": 5},
        {"my_id": 6},
    ]

    edges = [
        {"my_id_l": 1, "my_id_r": 2, "match_probability": 0.8},
        {"my_id_l": 3, "my_id_r": 2, "match_probability": 0.9},
        {"my_id_l": 4, "my_id_r": 5, "match_probability": 0.99},
    ]

    threshold_probabilities = [0.5, 0.7, 0.95]
    thresholds_weights = [0.0, 1.22, 4.25]

    cc_prob = cluster_pairwise_predictions_at_multiple_thresholds(
        nodes,
        edges,
        node_id_column_name="my_id",
        db_api=db_api,
        match_probability_thresholds=threshold_probabilities,
        output_cluster_summary_stats=False,
    )

    cc_prob_pd = cc_prob.as_pandas_dataframe()
    cc_prob_pd = cc_prob_pd.sort_values("my_id")

    cc_weight = cluster_pairwise_predictions_at_multiple_thresholds(
        nodes,
        edges,
        node_id_column_name="my_id",
        db_api=db_api,
        match_weight_thresholds=thresholds_weights,
        output_cluster_summary_stats=False,
    )

    cc_weight_pd = cc_weight.as_pandas_dataframe()
    cc_weight_pd = cc_weight_pd.sort_values("my_id")

    assert "cluster_mw_0" in cc_weight_pd.columns
    assert "cluster_mw_1_22" in cc_weight_pd.columns
    assert "cluster_mw_4_25" in cc_weight_pd.columns

    cc_prob_pd = cc_prob_pd.reset_index(drop=True)
    cc_weight_pd = cc_weight_pd.reset_index(drop=True)
    cc_weight_pd.columns = cc_prob_pd.columns

    pd.testing.assert_frame_equal(cc_prob_pd, cc_weight_pd)

    cc_prob_summary = cluster_pairwise_predictions_at_multiple_thresholds(
        nodes,
        edges,
        node_id_column_name="my_id",
        db_api=db_api,
        match_probability_thresholds=threshold_probabilities,
        output_cluster_summary_stats=True,
    )

    cc_prob_summary_pd = cc_prob_summary.as_pandas_dataframe()

    cc_weight_summary = cluster_pairwise_predictions_at_multiple_thresholds(
        nodes,
        edges,
        node_id_column_name="my_id",
        db_api=db_api,
        match_weight_thresholds=thresholds_weights,
        output_cluster_summary_stats=True,
    )

    cc_weight_summary_pd = cc_weight_summary.as_pandas_dataframe()

    # Check that num_clusters max_cluster_size avg_cluster_size contain same values
    pd.testing.assert_series_equal(
        cc_prob_summary_pd["num_clusters"], cc_weight_summary_pd["num_clusters"]
    )
    pd.testing.assert_series_equal(
        cc_prob_summary_pd["max_cluster_size"], cc_weight_summary_pd["max_cluster_size"]
    )
    pd.testing.assert_series_equal(
        cc_prob_summary_pd["avg_cluster_size"], cc_weight_summary_pd["avg_cluster_size"]
    )

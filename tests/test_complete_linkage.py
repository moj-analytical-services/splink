"""Tests for complete-linkage clustering.

Key property tested: a cluster produced by complete-linkage has no two nodes
whose directly-observed edge has match_probability < threshold, even if
connected components would have merged them via a chain of high-probability
edges ("chaining").
"""

import pandas as pd

from splink import DuckDBAPI
from splink.clustering import (
    cluster_pairwise_predictions_at_threshold,
    cluster_pairwise_predictions_at_threshold_complete_linkage,
)


def _cluster_map(df, id_col="id"):
    """Return {node_id: cluster_id} dict from a pandas dataframe."""
    return dict(zip(df[id_col], df["cluster_id"]))


# ---------------------------------------------------------------------------
# Core behavioural tests
# ---------------------------------------------------------------------------


def test_complete_linkage_splits_chained_cluster():
    """Triangle: 1-2 high, 2-3 high, 1-3 low.

    Connected components merges all three (1→2→3 path above threshold).
    Complete linkage must separate 1 from 3 because their direct edge is below
    the threshold.

    With 1-2 (0.90) < 2-3 (0.95), the weakest E_work edge is 1-2.
    After removing 1-2 from the working set:
      - CC gives {1} and {2, 3}.
      - No more intra-cluster conflicts (1-3 is now inter-cluster).
    Expected result: nodes 2 and 3 share a cluster; node 1 is alone.
    """
    db_api = DuckDBAPI()

    nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.90},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        {"id_l": 1, "id_r": 3, "match_probability": 0.30},
    ]
    threshold = 0.80

    # Connected components puts all three in the same cluster.
    cc_df = cluster_pairwise_predictions_at_threshold(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()
    assert (
        cc_df["cluster_id"].nunique() == 1
    ), "Connected components should put all three nodes in one cluster"

    # Complete linkage must separate node 1 from node 3.
    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    clusters = _cluster_map(cl_df)
    assert (
        cl_df["cluster_id"].nunique() == 2
    ), "Complete linkage should produce exactly 2 clusters"
    # 2 and 3 must be together; 1 must be separate.
    assert clusters[2] == clusters[3], "Nodes 2 and 3 should share a cluster"
    assert clusters[1] != clusters[2], "Node 1 should be in its own cluster"


def test_complete_linkage_no_conflicts_matches_connected_components():
    """When no below-threshold edges exist within a cluster, both algorithms
    produce the same result."""
    db_api = DuckDBAPI()

    nodes = [{"id": i} for i in range(1, 6)]
    # A simple chain: 1-2-3 above threshold; 4-5 above threshold; no cross-group edges.
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.95},
        {"id_l": 2, "id_r": 3, "match_probability": 0.92},
        {"id_l": 4, "id_r": 5, "match_probability": 0.99},
    ]
    threshold = 0.90

    cc_df = cluster_pairwise_predictions_at_threshold(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    cc_clusters = _cluster_map(cc_df)
    cl_clusters = _cluster_map(cl_df)

    # Both algorithms should produce the same partitioning.
    assert cc_clusters == cl_clusters


def test_complete_linkage_no_edges():
    """All nodes are singletons when the edge table is empty."""
    db_api = DuckDBAPI()

    nodes = [{"id": i} for i in range(1, 4)]
    # An empty edge table must still have the expected columns so that the
    # backend can register it (DuckDB cannot infer schema from an empty list).
    edges = pd.DataFrame(columns=["id_l", "id_r", "match_probability"])

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=0.9,
    ).as_pandas_dataframe()

    assert len(cl_df) == 3
    assert cl_df["cluster_id"].nunique() == 3


def test_complete_linkage_all_edges_below_threshold():
    """When every edge is below the threshold, all nodes become singletons."""
    db_api = DuckDBAPI()

    nodes = [{"id": i} for i in range(1, 4)]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.3},
        {"id_l": 2, "id_r": 3, "match_probability": 0.4},
    ]

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=0.9,
    ).as_pandas_dataframe()

    assert cl_df["cluster_id"].nunique() == 3


def test_complete_linkage_chain_without_conflict():
    """A chain A-B-C where only A-B and B-C are compared (A-C never compared).
    Both algorithms should produce a single cluster {A, B, C} because there is
    no observed conflict edge."""
    db_api = DuckDBAPI()

    nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.95},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        # No edge between 1 and 3 at all.
    ]
    threshold = 0.90

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    # All three should be in the same cluster — no conflict was observed.
    assert cl_df["cluster_id"].nunique() == 1


def test_complete_linkage_multiple_conflicts():
    """Multiple overlapping conflict pairs are resolved correctly.

    Graph (threshold = 0.8):
      1-2 (0.90), 2-3 (0.95), 3-4 (0.90)   <- E_work edges
      1-3 (0.20), 2-4 (0.10)                 <- conflict edges

    Connected components: {1, 2, 3, 4} (all linked via chain).
    The conflicts 1-3 and 2-4 force the cluster to split.

    Iteration 1:
      Conflicted cluster: {1,2,3,4}.
      Minimum E_work edge: 1-2 or 3-4 (both 0.90) < 2-3 (0.95).
      Tie-break by left id: remove 1-2.
      E_work: {2-3, 3-4}.

    Iteration 2:
      CC: {1} and {2,3,4}.
      Conflict 1-3: 1 and 3 now in different clusters → OK.
      Conflict 2-4: both in {2,3,4} → still a conflict.
      Remove minimum E_work edge in {2,3,4}: 3-4 or 2-3, both 0.9 or 0.95.
        Minimum is 3-4 (0.90). Remove it.
      E_work: {2-3}.

    Iteration 3:
      CC: {1}, {2,3}, {4}.
      Conflict 2-4: 2 and 4 in different clusters → OK.
      No conflicts. Done.

    Expected: {1}, {2, 3}, {4}  — three clusters.
    """
    db_api = DuckDBAPI()

    nodes = [{"id": i} for i in range(1, 5)]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.90},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        {"id_l": 3, "id_r": 4, "match_probability": 0.90},
        {"id_l": 1, "id_r": 3, "match_probability": 0.20},
        {"id_l": 2, "id_r": 4, "match_probability": 0.10},
    ]
    threshold = 0.80

    cc_df = cluster_pairwise_predictions_at_threshold(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()
    # Connected components puts all four together.
    assert cc_df["cluster_id"].nunique() == 1

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    clusters = _cluster_map(cl_df)
    # 1 should be isolated, 4 should be isolated, 2 and 3 together.
    assert cl_df["cluster_id"].nunique() == 3
    assert clusters[2] == clusters[3]
    assert clusters[1] != clusters[2]
    assert clusters[4] != clusters[2]
    assert clusters[1] != clusters[4]


def test_complete_linkage_independent_clusters_unaffected():
    """Clusters with no conflicts are not disturbed by splits in other clusters."""
    db_api = DuckDBAPI()

    nodes = [{"id": i} for i in range(1, 7)]
    edges = [
        # Group A: clean triangle — all pairs high probability.
        {"id_l": 1, "id_r": 2, "match_probability": 0.95},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        {"id_l": 1, "id_r": 3, "match_probability": 0.92},
        # Group B: conflicted triangle — 4-6 low.
        {"id_l": 4, "id_r": 5, "match_probability": 0.90},
        {"id_l": 5, "id_r": 6, "match_probability": 0.95},
        {"id_l": 4, "id_r": 6, "match_probability": 0.30},
    ]
    threshold = 0.80

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=threshold,
    ).as_pandas_dataframe()

    clusters = _cluster_map(cl_df)

    # Group A: 1, 2, 3 all in the same cluster (no conflicts).
    assert clusters[1] == clusters[2] == clusters[3]

    # Group B: 4 separated from 6 (direct conflict edge 4-6 = 0.30).
    assert clusters[4] != clusters[6]
    # 5 and 6 stay together (5-6 = 0.95, no conflict within {5,6}).
    assert clusters[5] == clusters[6]

    # Group A and Group B are in different clusters.
    assert clusters[1] != clusters[4]


def test_complete_linkage_match_weight_threshold():
    """threshold_match_weight is accepted and converted correctly."""
    db_api = DuckDBAPI()

    nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.90},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        {"id_l": 1, "id_r": 3, "match_probability": 0.30},
    ]

    # match_weight ≈ log2(0.8/0.2) = 2.0 → threshold_match_probability ≈ 0.8
    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_weight=2.0,
    ).as_pandas_dataframe()

    clusters = _cluster_map(cl_df)
    # Same structural assertion as the triangle test.
    assert clusters[2] == clusters[3]
    assert clusters[1] != clusters[2]


def test_complete_linkage_threshold_none_matches_connected_components():
    """With no threshold, complete linkage behaves identically to CC."""
    db_api = DuckDBAPI()

    nodes = [{"id": 1}, {"id": 2}, {"id": 3}]
    edges = [
        {"id_l": 1, "id_r": 2, "match_probability": 0.90},
        {"id_l": 2, "id_r": 3, "match_probability": 0.95},
        {"id_l": 1, "id_r": 3, "match_probability": 0.30},
    ]

    cc_df = cluster_pairwise_predictions_at_threshold(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=None,
    ).as_pandas_dataframe()

    cl_df = cluster_pairwise_predictions_at_threshold_complete_linkage(
        nodes,
        edges,
        db_api=db_api,
        node_id_column_name="id",
        edge_id_column_name_left="id_l",
        edge_id_column_name_right="id_r",
        threshold_match_probability=None,
    ).as_pandas_dataframe()

    assert _cluster_map(cc_df) == _cluster_map(cl_df)

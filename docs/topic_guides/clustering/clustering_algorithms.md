# Clustering Algorithms

Once pairwise match probabilities have been estimated, Splink groups records into clusters — one cluster per real-world entity. Two algorithms are available in [`splink.clustering`](../../api_docs/clustering.md).

## Connected components (default)

`cluster_pairwise_predictions_at_threshold` uses the **connected components** algorithm. Two records end up in the same cluster if there is *any* path of edges at or above the threshold connecting them.

```python
from splink import DuckDBAPI
from splink.clustering import cluster_pairwise_predictions_at_threshold

clusters = cluster_pairwise_predictions_at_threshold(
    nodes,
    edges,
    db_api=DuckDBAPI(),
    node_id_column_name="unique_id",
    threshold_match_probability=0.9,
)
```

This is fast and works well for most linkage tasks. The limitation is **chaining**: if A≈B and B≈C, all three are merged even when the A–C pair was compared and found to be a poor match.

## Complete linkage

`cluster_pairwise_predictions_at_threshold_complete_linkage` uses a stricter rule: a cluster is only valid when *every directly-observed edge* between nodes within the cluster meets the threshold. If any pair of nodes in the same candidate cluster has a known below-threshold edge, the cluster is split.

```python
from splink import DuckDBAPI
from splink.clustering import (
    cluster_pairwise_predictions_at_threshold_complete_linkage,
)

clusters = cluster_pairwise_predictions_at_threshold_complete_linkage(
    nodes,
    edges,
    db_api=DuckDBAPI(),
    node_id_column_name="unique_id",
    threshold_match_probability=0.9,
)
```

### The chaining problem

Consider three records where A–B and B–C both score above the threshold, but A–C was compared and scored well below it:

| Edge | match_probability |
|------|-------------------|
| A–B  | 0.95 |
| B–C  | 0.92 |
| A–C  | 0.15 |

Connected components chains them into one cluster `{A, B, C}` because A and C are transitively linked through B. Complete linkage detects the A–C conflict and splits the cluster, yielding `{B, C}` and `{A}`.

### Key requirement: include below-threshold edges

Complete linkage needs to *see* the conflict edges in order to detect them. The `edges` table must include all compared pairs — not just those above the threshold — along with their `match_probability`.

The most common mistake is passing the output of `linker.predict(threshold_match_probability=0.8)` directly to this function. That call returns only above-threshold edges, so there are no conflicts to detect and the result is silently identical to connected components.

Instead, use an unfiltered predictions table — or one filtered to a probability well below your clustering threshold — so that below-threshold edges are present:

```python
# Wrong: pre-filtering removes conflict edges
predictions = linker.predict(threshold_match_probability=0.8)

# Right: keep all (or most) compared pairs so conflicts are visible
predictions = linker.predict()

clusters = cluster_pairwise_predictions_at_threshold_complete_linkage(
    nodes,
    predictions,
    db_api=db_api,
    node_id_column_name="unique_id",
    threshold_match_probability=0.8,   # applied internally
)
```

The algorithm will warn at runtime if it detects that the edges table contains no below-threshold edges.

Pairs that were **never compared** due to blocking are treated as unknown (not as conflicts). The complete-linkage guarantee is scoped to *directly-observed* edges only: if A and C were blocked out and never compared, the algorithm will not treat their absence as a conflict.

### When to use complete linkage

- Your blocking rules generate **all-pairs comparisons** for candidate clusters, so below-threshold edges within a cluster are actually observed.
- You have reason to believe chaining is introducing false positive merges.
- Cluster quality is more important than clustering speed (complete linkage iterates until no conflicts remain, so it is slower than a single connected-components pass).

When none of the above apply, connected components is the right default.

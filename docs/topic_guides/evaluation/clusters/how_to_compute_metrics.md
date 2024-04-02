# How to compute graph metrics with Splink

To enable users to calculate a variety of graph metrics for their linked data, Splink provides the `compute_graph_metrics()` method.

The method is called on the `linker` like so:

```
linker.computer_graph_metrics(df_predict, df_clustered, threshold_match_probability=0.95)
```
with arguments

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`
        df_clustered (SplinkDataFrame): The outputs of
            `linker.cluster_pairwise_predictions_at_threshold()`
        threshold_match_probability (float): Filter the pairwise match predictions
            to include only pairwise comparisons with a match_probability at or
            above this threshold.

!!! warning

    `threshold_match_probability` should be the same as the clustering threshold passed to `cluster_pairwise_predictions_at_threshold()`. If this information is available to Splink then it will be passed automatically, otherwise the user will have to provide it themselves and take care to ensure that threshold values align.

The method generates tables containing graph metrics (for nodes, edges and clusters), and returns a data class of [Splink dataframes](../../../SplinkDataFrame.md). The individual Splink dataframes containing node, edge and cluster metrics can be accessed as follows:

```
compute_graph_metrics.nodes for node metrics
compute_graph_metrics.edges for edge metrics
compute_graph_metrics.clusters for cluster metrics
```

The metrics computed by `compute_graph_metrics()` include all those mentioned in the [Graph metrics]() chapter, namely:

* Node degree
* 'Is bridge'
* Cluster size
* Cluster density
* Cluster centrality

All of these metrics are calculated by default. If you are unable to install the `igraph` package required for 'is bridge', this metric won't be calculated, however all other metrics will still be generated.

This topic guide is a work in progress. Please check back for more detailed examples of how `compute_graph_metrics()` can be used to evaluate linked data.

# How to compute graph metrics with Splink

To enable users to calculate a variety of graph metrics for their linked data, Splink provides the `compute_graph_metrics()` method.

    """
    Generates tables containing graph metrics (for nodes, edges and clusters),
    and returns a data class of Splink dataframes

    Args:
        df_predict (SplinkDataFrame): The results of `linker.predict()`
        df_clustered (SplinkDataFrame): The outputs of
            `linker.cluster_pairwise_predictions_at_threshold()`
        threshold_match_probability (float): Filter the pairwise match predictions
            to include only pairwise comparisons with a match_probability at or
            above this threshold.

    Returns:
        GraphMetricsResult: A data class containing SplinkDataFrames
        of cluster IDs and selected node, edge or cluster metrics.
            attribute "nodes" for nodes metrics table
            attribute "edges" for edge metrics table
            attribute "clusters" for cluster metrics table

    """

The `threshold_match_probability` provided should be the same as the clustering threshold passed to `cluster_pairwise_predictions_at_threshold()`. If this information is available to Splink then it will be passed automatically, otherwise the user will have to provide it themselves and take care to ensure that threshold values align.

As stated above, `compute_graph_metrics()` returns a set of Splink Dataframes. The individual Splink Dataframes containing node, edge and cluster metrics (as introduced and defined in [Graph metrics]()) can be accessed as follows

    """
    `compute_graph_metrics.nodes` for node metrics
    `compute_graph_metrics.edges` for edge metrics
    `compute_graph_metrics.clusters` for cluster metrics
    """

The metrics which are computed by `compute_graph_metrics()` include all those mentioned in [Graph metrics](), namely

* Cluster size
* Cluster density
* Node degree
* Cluster centrality
* 'is bridge'

All of these metrics are calculated by default. If you are unable to install the packages...required to compute 'is bridge', this metric won't be calculated, however all other metrics will still be produced.

This topic guide is a work in progress.
Worked through example of computing metrics and applying metrics to evaluate and improve cluster quality.

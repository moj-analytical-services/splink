# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: splink-bxsLLt4m
#     language: python
#     name: python3
# ---

# %% [markdown]
# # How to compute graph metrics with Splink

# %% [markdown]
# ## Introduction to the `compute_graph_metrics()` method

# %% [markdown]
#
#

# %% [markdown]
# To enable users to calculate a variety of graph metrics for their linked data, Splink provides the `compute_graph_metrics()` method.
#
# The method is called on the `linker` like so:
#
# ```
# linker.clustering.compute_graph_metrics(df_predict, df_clustered, threshold_match_probability=0.95)
# ```
#
# ::: splink.internals.linker_components.clustering.LinkerClustering.compute_graph_metrics
#     handler: python
#     options:
#       show_root_heading: false
#       show_root_toc_entry: false
#       show_source: false
#       show_docstring_parameters: true
#       show_docstring_description: false
#       show_docstring_returns: false
#       show_docstring_examples: false
#       members_order: source
#       
# !!! warning
#
#     `threshold_match_probability` should be the same as the clustering threshold passed to `cluster_pairwise_predictions_at_threshold()`. If this information is available to Splink then it will be passed automatically, otherwise the user will have to provide it themselves and take care to ensure that threshold values align.
#
# The method generates tables containing graph metrics (for nodes, edges and clusters), and returns a data class of [Splink dataframes](../../../api_docs/splink_dataframe.md). The individual Splink dataframes containing node, edge and cluster metrics can be accessed as follows:
#
# ```python
# graph_metrics = linker.clustering.compute_graph_metrics(
#     pairwise_predictions, clusters
# )
#
# df_edges = graph_metrics.edges.as_pandas_dataframe()
# df_nodes = graph_metrics.nodes.as_pandas_dataframe()
# df_clusters = graph_metrics.clusters.as_pandas_dataframe()
#
# ```
#
# The metrics computed by `compute_graph_metrics()` include all those mentioned in the [Graph metrics](./graph_metrics.md) chapter, namely:
#
# * Node degree
# * Node centrality
# * 'Is bridge'
# * Cluster size
# * Cluster density
# * Cluster centrality
#
# All of these metrics are calculated by default. If you are unable to install the `igraph` package required for 'is bridge', this metric won't be calculated, however all other metrics will still be generated.
#
#

# %% [markdown]
# ## Full code example

# %% [markdown]
# This code snippet computes graph metrics for a simple Splink dedupe model. A pandas dataframe of cluster metrics is displayed as the final output.

# %% tags=["hide_output"]
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

df = splink_datasets.historical_50k

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch(
            "first_name",
        ).configure(term_frequency_adjustments=True),
        cl.JaroWinklerAtThresholds("surname", score_threshold_or_thresholds=[0.9, 0.8]),
        cl.LevenshteinAtThresholds(
            "postcode_fake", distance_threshold_or_thresholds=[1, 2]
        ),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("postcode_fake", "first_name"),
        block_on("first_name", "surname"),
        block_on("dob", "substr(postcode_fake,1,2)"),
        block_on("postcode_fake", "substr(dob,1,3)"),
        block_on("postcode_fake", "substr(dob,4,5)"),
    ],
    retain_intermediate_calculation_columns=True,
)

db_api = DuckDBAPI()
df_sdf = db_api.register(df)
linker = Linker(df_sdf, settings)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name", "surname")
)

linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("dob", "substr(postcode_fake, 1,3)")
)

pairwise_predictions = linker.inference.predict()
clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    pairwise_predictions, 0.95
)

graph_metrics = linker.clustering.compute_graph_metrics(pairwise_predictions, clusters)

df_clusters = graph_metrics.clusters.as_pandas_dataframe()


# %%
df_clusters

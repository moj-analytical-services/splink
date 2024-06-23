# Evaluation Overview

Evaluation is a non-trivial, but crucial, task in data linkage. Linkage pipelines are complex and require many design decisions, each of which has an impact on the end result. 

This set of topic guides is intended to provide some structure and guidance on how to evaluate a Splink model alongside its resulting links and clusters.

## How do we evaluate different stages of the pipeline?

Evaluation in a data linking pipeline can be broken into 3 broad categories:

### :material-chart-tree: Model Evaluation

After you have trained your model, you can start evaluating the parameters and overall design of the model. To see how, check out the [Model Evaluation guide](./model.md).

### :octicons-link-16: Edge (Link) Evaluation

Once you have trained a model, you will use it to predict the probability of links (edges) between entities (nodes). To see how to evaluate these links, check out the [Edge Evaluation guide](./edge_overview.md).

### :fontawesome-solid-circle-nodes: Cluster Evaluation

Once you have chosen a linkage threshold, the edges are used to generate clusters of records. To see how to evaluate these clusters, check out the [Cluster Evaluation guide](./clusters/overview.md).

<hr>

!!! note

    In reality, the development of a linkage pipeline involves iterating through multiple versions of models, links and clusters. For example, for each model version you will generally want to understand the downstream impact on the links and clusters generated. As such, you will likely revisit each stage of evaluation a number of times before settling on a final output.

    The aim of these guides, and the tools provided in Splink, is to ensure that you are able to extract enough information from each iteration to better understand how your pipeline is working and identify areas for improvement.



# Edge Evaluation

Once you have a trained model, you use it to generate edges (links) between entities (nodes). These edges will have a Match Weight and corresponding Probability.

There are several strategies for checking whether the links created in your pipeline perform as you want/expect.

## Consider the Edge Metrics

Edge Metrics measure how links perform at an overall level.

First, consider how you would like your model to perform. What is important for your use case? Do you want to ensure that you capture all possible matches (i.e. high recall)? Or do you want to minimise the number of incorrectly predicted matches (i.e. high precision)? Perhaps a combination of both?

For a summary of all the edge metrics available in Splink, check out the [Edge Metrics guide](./edge_metrics.md).

!!! note

    To produce Edge Metrics you will require a "ground truth" to compare your linkage results against (which can be achieved by [Clerical Labelling](./labelling.md)).


## Spot Checking pairs of records

Spot Checking real examples of record pairs is helpful for confidence in linkage results. It is an effective way to build intuition for how the model works in practice and allows you to interrogate edge cases.

Results of individual record pairs can be examined with the [Waterfall Chart](../../charts/waterfall_chart.ipynb).

Choosing which pairs of records to spot check can be done by either:

- Looking at all combinations of comparison levels and choosing which to examine in the [Comparison Viewer Dashboard](../../charts/comparison_viewer_dashboard.ipynb).
- Identifying and examining records which have been [incorrectly predicted by your Splink model](../../demos/examples/duckdb/accuracy_analysis_from_labels_column.ipynb).

As you are checking real examples, you will often come across cases that have not been accounted for by your model which you believe signify a match (e.g. a fuzzy match for names). We recommend using this feedback loop to help iterate and improve the definition of your model.

## Choosing a Threshold

Threshold selection is a key decision point within a linkage pipeline. One of the major benefits of probabilistic linkage versus a deterministic (i.e. rules-based) approach is the ability to choose the amount of evidence required for two records to be considered a match (i.e. a threshold).

When you have decided on the metrics that are important for your use case, you can use the [Threshold Selection Tool](../../charts/threshold_selection_tool_from_labels_table.ipynb) to get a first estimate for what your threshold should be.

!!! note

    The Threshold Selection Tool requires labelled data to act as a "ground truth" to compare your linkage results against.

Once you have an initial threshold, you can use [Comparison Viewer Dashboard](../../charts/comparison_viewer_dashboard.ipynb) to look at records on either side of your threshold to check whether the threshold makes intuitive sense.

From here, we recommend an iterative process of tweaking your threshold based on your spot checking then looking at the impact that this has on your overall edge metrics. Another tools that can be useful is spot checking where your model has gone wrong using [`prediction_errors_from_labels_table`](../../api_docs/evaluation.md) as demoed in the [accuracy analysis demo](../../demos//examples/duckdb/accuracy_analysis_from_labels_column.ipynb).


## In Summary

Evaluating the edges (links) of a linkage model depends on your use case. Defining what "good" looks like is a key step, which then allows you to choose a relevant metric (or metrics) for measuring success.

Your desired metric should help give an initial estimation for a linkage threshold, then you can use spot checking to help settle on a final threshold.

In general, the links between pairs of records are not the final output of linkage pipeline. Most use-cases use these links to group records together into clusters. In this instance, evaluating the links themselves is not sufficient, you have to [evaluate the resulting clusters as well](./clusters/overview.md).
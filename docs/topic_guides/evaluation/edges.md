# Edge Evaluation

Once you have a trained model, you use it to generate edges (links) between entities (nodes). These edges will have a Match Weight and corresponding Probability.

There are two distinct types of Edge Evaluation that should be considered at this stage:

1. Edge Metrics
2. Spot checking

with the insights from both of these feeding into Threshold Selection ahead of of the Clustering stage.

<hr>

## Edge Metrics

Edge Metrics measure for how the links perform at an overall level.

 For a summary of all the edge metrics available in Splink, check out the [metrics guide](./edge_metrics.md). These metrics require:

 - a probability threshold threshold to be chosen
 - a "ground truth" to compare against (which can be achieved by Clerical Labelling)

## Spot Checking



## Threshold Selection


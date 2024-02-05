# Edge Evaluation

Once you have a trained model, you use it to generate edges (links) between entities (nodes). These edges will have a Match Weight and corresponding Probability.

There are two distinct types of Edge Evaluation that should be considered at this stage:

1. [Edge Metrics](#edge-metrics)
2. [Spot Checking](#spot-checking)

with the insights from both of these feeding into [Threshold Selection](#threshold-selection) ahead of of the Clustering stage.

<hr>

## Edge Metrics

Edge Metrics measure for how the links perform at an overall level.

 For a summary of all the edge metrics available in Splink, check out the [metrics guide](./edge_metrics.md). These metrics require:

 - a probability threshold threshold to be chosen
 - a "ground truth" to compare against (which can be achieved by Clerical Labelling)

## Spot Checking

Spot checking real examples of pairs of records is a central pillar of evaluating a linkage pipeline. Unlike the overall Edge Metrics, you do not get a clear measure of performance. It is, however, a very effective way to build up confidence in the model by looking at specific examples of outputs (including edge cases). It helps to build up an intuition for how the model works in practice, similar to the steps in [Model Evaluation](./model.md).


## Threshold Selection

Threshold selection is a key part of a linkage pipeline. 


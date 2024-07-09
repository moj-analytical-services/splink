---
tags:
  - Performance
---


This topic guide covers the fundamental drivers of the run time of Splink jobs.

## Blocking

The primary driver of run time is **the number of record pairs that the Splink model has to process**. In Splink, the number of pairs to consider is reduced using **Blocking Rules** which are covered in depth in their own set of [topic guides](../blocking/blocking_rules.md).

## Complexity of comparisons

More complex comparisons reduces performance. Complexity is added to comparisons in a number of ways, including:

- Increasing the number of comparison levels
- Using more computationally expensive comparison functions
- Adding Term Frequency Adjustments

!!! note "Performant Term Frequency Adjustments"
    Model training with Term Frequency adjustments can be made more performant by setting `estimate_without_term_frequencies` parameter to `True` in `estimate_parameters_using_expectation_maximisation`.

## Retaining columns through the linkage process

The size your dataset has an impact on the performance of Splink. This is also applicable to the tables that Splink creates and uses under the hood. Some Splink functionality requires additional calculated columns to be stored. For example:

- The `comparison_viewer_dashboard` requires `retain_matching_columns` and `retain_intermediate_calculation_columns` to be set to `True` in the settings dictionary, but this makes some processes less performant.

## Filtering out pairwise in the `predict()` step

Reducing the number of pairwise comparisons that need to be returned will make Splink perform faster. One way of doing this is to filter comparisons with a match score below a given threshold (using a `threshold_match_probability` or `threshold_match_weight`) when you call `predict()`.

## Spark Performance

As :simple-apachespark: Spark is designed to distribute processing across multiple machines so there are additional configuration options available to make jobs run more quickly. For more information, check out the [Spark Performance Topic Guide](./optimising_spark.md).

!!! tip "Balancing computational performance and model accuracy"

    There is usually a trade off between performance and accuracy in Splink models. I.e. some model design decisions that improve computational performance can also have a negative impact the accuracy of the model.

    Be sure to check how the suggestions in this topic guide impact the accuracy of your model to ensure the best results.
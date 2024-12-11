---
tags:
  - Performance
---


This topic guide covers the fundamental drivers of the run time of Splink jobs.

## Blocking

The primary driver of run time is **the number of record pairs that the Splink model has to process**. In Splink, the number of pairs to consider is reduced using **Blocking Rules** which are covered in depth in their own set of [topic guides](../blocking/blocking_rules.md).

## Complexity of comparisons

The second most important driver of runtime is the complexity of comparisons, and the computional intensity of the fuzzy matching functions used.

Complexity is added to comparisons in a number of ways, including:

- Increasing the number of comparison levels
- Using more computationally expensive comparison functions
- Adding Term Frequency Adjustments

See [performance of comparison functions](../performance/performance_of_comparison_functions.ipynb) for benchmarking results.

## Retaining columns through the linkage process

The size your dataset has an impact on the performance of Splink. This is also applicable to the tables that Splink creates and uses under the hood. Some Splink functionality requires additional calculated columns to be stored. For example:

- The `comparison_viewer_dashboard` requires `retain_matching_columns` and `retain_intermediate_calculation_columns` to be set to `True` in the settings dictionary, but this makes some processes less performant.

## Filtering out pairwise comparisons in the `predict()` step

Reducing the number of pairwise comparisons that need to be returned will make Splink perform faster. One way of doing this is to filter comparisons with a match score below a given threshold (using a `threshold_match_probability` or `threshold_match_weight`) when you call `predict()`.

## Model training without term frequency adjustments

Model training with Term Frequency adjustments can be made more performant by setting `estimate_without_term_frequencies` parameter to `True` in `estimate_parameters_using_expectation_maximisation`.





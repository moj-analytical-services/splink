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

## Sampling to speed up model training and blocking analysis

On large datasets, several of the model-training and blocking-analysis steps can be slow to run exactly. Each accepts an argument that trades a little accuracy for a faster run time:

- `count_comparisons_from_blocking_rules` and `chart_comparisons_from_blocking_rules` accept `record_sample_proportion` to estimate comparison counts from a sample of input records rather than scanning the whole dataset.
- `estimate_u_using_random_sampling` uses `max_pairs` to control how many random pairs are sampled when estimating the `u` probabilities.
- `estimate_parameters_using_expectation_maximisation` accepts `max_pairs` to cap the number of blocked pairs used in each EM training session, without having to tighten your blocking rule.
- `estimate_probability_two_random_records_match` accepts `record_sample_proportion` to estimate the deterministic match count from a sample.

## Chunking `predict()` to control memory and distribute work

`predict()` generates and scores every blocked pair in a single pass. On very large jobs this can use a lot of memory, or run out of memory or disk entirely. You can split the work into deterministic chunks by passing `num_chunks_left` and `num_chunks_right` to `predict()`: Splink processes the chunks one at a time and unions the results, lowering peak memory and logging progress as it goes. The return value is identical to an unchunked `predict()`.

Because each chunk is self-contained, you can also score chunks independently — even on different machines — using `predict_chunk()`, and combine the per-chunk outputs yourself.

See the [scaling up to large datasets tutorial](../../demos/tutorials/09_scaling_up_techniques.ipynb) for worked examples of both sampling and chunking.





---
tags:
  - Performance
---


This topic guide covers the fundamental drivers of the run time of Splink jobs. 

The primary driver of run time is **the number of record pairs that the Splink model has to process**. In Splink, the number of pairs to consider is reduced using **Blocking Rules** which are covered in depth in their own set of [topic guides](./blocking_rules.md). 

Additional factors which affect performance are:

- the complexity of your comparisons e.g. whether you apply term frequency adjustments
- whether you choose to set `retain_matching_columns` and `retain_intermediate_calculation_columns` to `True` in your settings,
- whether you filter out comparisons with a match score below a given threshold (using a `threshold_match_probability` or `threshold_match_weight` when you call `predict()`).

## :simple-apachespark: Spark Performance

As :simple-apachespark: Spark is designed to distribute processing across multiple machines so there are additional configuration options available to make jobs run more quickly. For more information, check out the [Spark Performance Topic Guide](./optimising_spark.md).
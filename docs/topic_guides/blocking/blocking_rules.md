---
tags:
  - Blocking
  - Performance
---

# What are Blocking Rules?

The primary driver the run time of Splink is the number of record pairs that the Splink model has to process.  This is controlled by the blocking rules.

This guide explains what blocking rules are, and how they can be used.

## Introduction

One of the main challenges to overcome in record linkage is the **scale** of the problem.

The number of pairs of records to compare grows using the formula $\frac{n\left(n-1\right)}2$, i.e. with (approximately) the square of the number of records, as shown in the following chart:

![](../../img/blocking/pairwise_comparisons.png)

For example, a dataset of 1 million input records would generate around 500 billion pairwise record comparisons.

So, when datasets get bigger the amount of computational resource gets extremely large (and costly). In reality, we try and reduce the amount of computation required using **blocking**.

## Blocking

Blocking is a technique for reducing the number of record pairs that are considered by a model.

Considering a dataset of 1 million records, comparing each record against all of the other records in the dataset generates ~500 billion pairwise comparisons. However, we know the vast majority of these record comparisons won't be matches, so processing the full ~500 billion comparisons would be largely pointless (as well as costly and time-consuming).

Instead, we can define a subset of potential comparisons using **Blocking Rules**. These are rules that define "blocks" of comparisons that should be considered. For example, the blocking rule:

`"block_on("first_name", "surname")`

will generate only those pairwise record comparisons where first name and surname match.  That is, is equivalent to joining input records the SQL condition  `l.first_name = r.first_name and l.surname = r.surname`

Within a Splink model, you can specify multiple Blocking Rules to ensure all potential matches are considered.  These are provided as a list.  Splink will then produce all record comparisons that satisfy at least one of your blocking rules.

???+ "Further Reading"

    For more information on blocking, please refer to [this article](https://toolkit.data.gov.au/data-integration/data-integration-projects/probabilistic-linking.html#key-steps-in-probabilistic-linking)

## Blocking in Splink

There are two areas in Splink where blocking is used:

- The first is to generate pairwise comparisons when finding links (running `predict()`). This is the sense in which 'blocking' is usually understood in the context of record linkage.  These blocking rules are provided in the model settings using `blocking_rules_to_generate_predictions`.

- The second is a less familiar application of blocking: using it for model training. This is a more advanced topic, and is covered in the [model training](./model_training.md) topic guide.


### Choosing `blocking_rules_to_generate_predictions`

The blocking rules specified in your settings at `blocking_rules_to_generate_predictions` are the single most important determinant of how quickly your linkage runs.  This is because the number of comparisons generated is usually many multiple times higher than the number of input records.

How can we choose a good set of blocking rules?

The aim of blocking rules is to recover (almost) all matching record pairs (i.e to have high recall).

It is less important if the blocking rules select some (or even many) record pairs which are not matches (i.e. that they achieve high precision). Record comparisons that 'pass' the blocking rules are then put forward to the scoring/prediction step, and so non-matching records will be rejected.

Nonetheless, the more pairs let through, the more computation is required at the prediction step.






!!! note "Taking blocking to the extremes"
    If you have a large dataset to deduplicate, let's consider the implications of two cases of taking blocking to the extremes:

    **Not enough blocking** (ensuring all matches are captured)
    There will be too many record pairs to consider, which will take an extremely long time to run (hours/days) or the process will be so large that it crashes.

    **Too much blocking** (minimising computational resource)
    There won't be enough records pairs to consider, so the model won't perform well (or will struggle to be trained at all).



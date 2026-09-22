---
tags:
  - Performance
  - DuckDB
---

## Optimising DuckDB jobs

This topic guide describes how to configure DuckDB to optimise performance

It is assumed readers have already read the more general [guide to linking big data](./drivers_of_performance.md), and have chosen appropriate blocking rules.

## Summary:

You will generally get best performance on a high core count Linux or Mac machine with lots of RAM.

These specific tips are some of the most important:

- Use an in-memory connection, and avoid spill to disk.  If your job is spilling to disk, you have two options to avoid this:
    - use the materialise-to-parquet options [todo: link to the relevant docs seciont].
    - Chunk your workload [todo: link to the relevant docs seciont].
- Use DuckDB 2.0, especially on large core-count machines.  Our testing suggest it paralellises subtantially better on machines with more than around 64 cores,
- Only use chunking if you need it.  Chunking results in some duplicated calculation.
- Performance on Mac and Linux is generally significantly better than Windows.
- Our of memory errors: In some cloud environments, the environment may not correctly report the amount of RAM available.  To avoid out of memory errors, you should explicitly [set the `memory_limit` pragma](https://duckdb.org/docs/current/configuration/pragmas#memory-limit) when creating the DuckDB connection.
- Store your data on an SSD and write out results to SSD.  If writing to S3, it may be faster to write to SSD and then sync to S3, see [here](https://www.robinlinacre.com/optimising_duckdb_performance_large_ec2_instances/).

The easiest way to tell if your job has significant room for optimisation is to look at CPU usage through time.  If you are not using 100% of all cores for most of the main Splink operations (blocking and prediction), there is likely room for improvement by changing your configuration.

You can find a historical blog post with formal benchmarks of DuckDB performance on a variety of machine types [here](https://www.robinlinacre.com/fast_deduplication/), though performance is now generally better than this.

## Configuration


We recommend using an in memory DuckDB connection `con = duckdb.connect(":memory:")` because it makes writing large tables faster.  In particular, writing to an on-disk DuckDB database is relatively expesive because of  [todo: look in duckdb latest codebase, there are various things to make writes robust to crashes that mean writing to duckdb database files is more expensive than writing to a temp in mem connection or direct to parquet.  Insert short comment here describing findings.]

### Running out of memory

If your job is running out of memory, then the first thing to consider is tightening your blocking rules [todo: link to the relevant docs section].

If this is not possible, you can relieve memory pressure whilst still using an in-memory connection using  the following options:

#### Materialise to parquet

Materialisation directly to parquet releves pressure because intermediate results are offloaded to disk

```python
todo: add example here

```
#### Chunking `predict()`

If the memory pressure comes from the `predict()` step, you can split it into smaller pieces using the `num_chunks_left` and `num_chunks_right` arguments. Splink processes the chunks in series and unions the results, so only a fraction of the blocked pairs are materialised at any one time. This also gives progress reporting on long-running jobs. See the [scaling up to large datasets tutorial](../../demos/tutorials/09_scaling_up_techniques.ipynb) for details.



## Avoiding repeated computation in comparisons

When you use a fuzzy comparison such as `JaroWinklerAtThresholds` with several thresholds, Splink generates a SQL `CASE` statement that calls the comparison function once for each threshold:

```sql
CASE
    WHEN "name_l" IS NULL OR "name_r" IS NULL THEN -1
    WHEN "name_l" = "name_r" THEN 4
    WHEN jaro_winkler_similarity("name_l", "name_r") >= 0.9 THEN 3
    WHEN jaro_winkler_similarity("name_l", "name_r") >= 0.8 THEN 2
    WHEN jaro_winkler_similarity("name_l", "name_r") >= 0.7 THEN 1
    ELSE 0
END
```

This suggests an obvious optimisation: compute the `jaro_winkler_similarity` value once and reuse it across thresholds.  This is sometimes called 'hoisting'.

DuckDB's optimiser does attempt to do this, but it will not usually work for Splink's `CASE` statements.

This is deliberate: DuckDB avoids hoisting when an earlier branch of the `CASE` is 'protecting' a later branch from running. Splink's null-handling branch is exactly such a case: the later branches may produce an error if one side is null.

As a motivating example:

```python
import duckdb
duckdb.sql("SELECT jaccard('', 'abc')")
# InvalidInputException: Jaccard Function: An argument too short!
```

If DuckDB hoisted `jaccard(...)` out of the `CASE` and evaluated it for every row, this query would error on the blank inputs that the null/blank check was there to skip. For this reason Splink cannot safely enable the optimisation for you automatically, and DuckDB is right not to apply it by default.

### Enabling the optimisation yourself

If you know your comparison function is safe to evaluate on every row (i.e. will not error), you can rewrite the comparison so the function appears in the first branch of the `CASE`. DuckDB will then recognise it's safe to hoist, compute it once, and reuse the result across all thresholds.

For example, `jaro_winkler_similarity` and `levenshtein` are safe in this way.

The trick is to move the function into the null level using a sentinel comparison that can never be true. Because Jaro-Winkler only returns values in the range `[0, 1]`, the test `= -100` never matches, so the rows captured by the null level are exactly the same as a plain `IS NULL` check:

```python
import splink.comparison_level_library as cll
import splink.comparison_library as cl

name_comparison = cl.CustomComparison(
    output_column_name="name",
    comparison_levels=[
        # The function appears in the first branch, so DuckDB computes it once.
        # `= -100` is never true, so the null logic is unchanged.
        cll.CustomLevel(
            "jaro_winkler_similarity(name_l, name_r) = -100 "
            "OR name_l IS NULL OR name_r IS NULL",
            label_for_charts="name is NULL",
        ).configure(is_null_level=True),
        cll.ExactMatchLevel("name"),
        cll.JaroWinklerLevel("name", 0.9),
        cll.JaroWinklerLevel("name", 0.8),
        cll.JaroWinklerLevel("name", 0.7),
        cll.ElseLevel(),
    ],
)
```

This produces identical results to `cl.JaroWinklerAtThresholds("name", [0.9, 0.8, 0.7])`, but DuckDB now evaluates `jaro_winkler_similarity` once per row instead of once per threshold. The more thresholds you use, and the more expensive the function, the larger the saving.

Setting `.configure(is_null_level=True)` is important: it tells Splink to continue treating this level as the null level, so that — exactly as for a standard `NullLevel` — its `m` and `u` values are not estimated during training.

For more information, see [here](https://github.com/moj-analytical-services/splink/pull/2738)


---
tags:
  - Performance
  - DuckDB
---

## Optimising DuckDB jobs

This topic guide describes how to configure DuckDB to optimise performance

It is assumed readers have already read the more general [guide to linking big data](./drivers_of_performance.md), and have chosen appropriate blocking rules.

## Summary:

- From `splink==3.9.11` onwards, DuckDB generally parallelises jobs well, so you should see 100% usage of all CPU cores for the main Splink operations (parameter estimation and prediction)
- If you are facing memory issues with DuckDB, you have the option of using an on-disk database.
- In some cloud environments, the environment may not correctly report the amount of RAM available, so if you're getting out of memory errors, you should explicitly [set the `memory_limit` pragma](https://duckdb.org/docs/current/configuration/pragmas#memory-limit) when creating the DuckDB connection.

You can find a blog post with formal benchmarks of DuckDB performance on a variety of machine types [here](https://www.robinlinacre.com/fast_deduplication/).

## Configuration

### Running out of memory

If your job is running out of memory, the first thing to consider is tightening your blocking rules, or running the workload on a larger machine.

If these are not possible, the following config options may help reduce memory usage:

#### Using an on-disk database

DuckDB can spill to disk using several settings:

Use the special `:temporary:` connection built into Splink that creates a temporary on disk database

```python

db_api = DuckDBAPI(connection=":temporary:")
df_sdf = db_api.register(df, dataset_display_name="my_data")
linker = Linker(df_sdf, settings)
```

Use an on-disk database:

```python
con = duckdb.connect(database='my-db.duckdb')
db_api = DuckDBAPI(connection=con)
df_sdf = db_api.register(df, dataset_display_name="my_data")
linker = Linker(df_sdf, settings)
```

Use an in-memory database, but ensure it can spill to disk:

```python
con = duckdb.connect(":memory:")

con.execute("SET temp_directory='/path/to/temp';")
db_api = DuckDBAPI(connection=con)
df_sdf = db_api.register(df, dataset_display_name="my_data")
linker = Linker(df_sdf, settings)
```

See also [this section](https://duckdb.org/docs/guides/performance/how-to-tune-workloads.html#larger-than-memory-workloads-out-of-core-processing) of the DuckDB docs

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

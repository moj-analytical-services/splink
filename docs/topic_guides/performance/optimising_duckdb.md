---
tags:
  - Performance
  - DuckDB
  - Salting
  - Parallelism
---

## Optimising DuckDB jobs

This topic guide describes how to configure DuckDB to optimise performance

It is assumed readers have already read the more general [guide to linking big data](./drivers_of_performance.md), and have chosen appropriate blocking rules.

## Summary:

- From `splink==3.9.11` onwards, DuckDB generally parallelises jobs well, so you should see 100% usage of all CPU cores for the main Splink operations (parameter estimation and prediction)
- In some cases `predict()` needs salting on `blocking_rules_to_generate_predictions` to achieve 100% CPU use. You're most likely to need this in the following scenarios:
    - Very high core count machines
    - Splink models that contain a small number of `blocking_rules_to_generate_predictions`
    - Splink models that have a relatively small number of input rows (less than around 500k)
- If you are facing memory issues with DuckDB, you have the option of using an on-disk database.
- Reducing the amount of parallelism by removing salting can also sometimes reduce memory usage

You can find a blog post with formal benchmarks of DuckDB performance on a variety of machine types [here](https://www.robinlinacre.com/fast_deduplication/).

## Configuration

### Ensuring 100% CPU usage across all cores on `predict()`

The aim is for overall parallelism of the predict() step to closely align to the number of thread/vCPU cores you have:
- If parallelism is too low, you won't use all your threads
- If parallelism is too high, runtime will be longer.

The number of CPU cores used is given by the following formula:

$\text{base parallelism} = \frac{\text{number of input rows}}{122,880}$

$\text{blocking rule parallelism}$

$= \text{count of blocking rules} \times$ $\text{number of salting partitions per blocking rule}$

$\text{overall parallelism} = \text{base parallelism} \times \text{blocking rule parallelism}$

If overall parallelism is less than the total number of threads, then you won't achieve 100% CPU usage.

#### Example

Consider a deduplication job with 1,000,000 input rows, on a machine with 32 cores (64 threads)

In our Splink suppose we set:

```python
settings =  {
    ...
    "blocking_rules_to_generate_predictions" ; [
        block_on("first_name", salting_partitions=2),
        block_on("dob", salting_partitions=2),
        block_on("surname", salting_partitions=2),
    ]
    ...
}
```

Then we have:

- Base parallelism of 9.
- 3 blocking rules
- 2 salting partitions per blocking rule

We therefore have paralleism of $9 \times 3 \times 2 = 54$, which is less than the 64 threads, and therefore we won't quite achieve full parallelism.

### Generalisation

The above formula for overall parallelism assumes all blocking rules have the same number of salting partitions, which is not necessarily the case. In the more general case of variable numbers of salting partitions, the formula becomes

$$
\text{overall parallelism} =
\text{base parallelism} \times \text{total number of salted blocking partitions across all blocking rules}
$$

So for example, with two blocking rules, if the first has 2 salting partitions, and the second has 10 salting partitions, when we would multiply base parallelism by 12.

This may be useful in the case one of the blocking rules produces more comparisons than another: the 'bigger' blocking rule can be salted more.

For further information about how parallelism works in DuckDB, including links to relevant DuckDB documentation and discussions, see [here](https://github.com/moj-analytical-services/splink/discussions/1830).

### Running out of memory

If your job is running out of memory, the first thing to consider is tightening your blocking rules, or running the workload on a larger machine.

If these are not possible, the following config options may help reduce memory usage:

#### Using an on-disk database

DuckDB can spill to disk using several settings:

Use the special `:temporary:` connection built into Splink that creates a temporary on disk database

```python

linker = Linker(
    df, settings, DuckDBAPI(connection=":temporary:")
)
```

Use an on-disk database:

```python
con = duckdb.connect(database='my-db.duckdb')
linker = Linker(
    df, settings, DuckDBAPI(connection=con)
)
```

Use an in-memory database, but ensure it can spill to disk:

```python
con = duckdb.connect(":memory:")

con.execute("SET temp_directory='/path/to/temp';")
linker = Linker(
    df, settings, DuckDBAPI(connection=con)
)
```

See also [this section](https://duckdb.org/docs/guides/performance/how-to-tune-workloads.html#larger-than-memory-workloads-out-of-core-processing) of the DuckDB docs

#### Reducing salting

Empirically we have noticed that there is a tension between parallelism and total memory usage. If you're running out of memory, you could consider reducing parallelism.

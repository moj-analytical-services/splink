## Caching and pipelining in Spark

This article assumes you've read the [general guide to caching and pipelining](https://moj-analytical-services.github.io/splink/dev_guides/caching.html).

In Spark, some additions have to be made to this general pattern because [all transformation in Spark are lazy](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations).

That is, when we call `df = spark.sql(sql)`, the `df` is not immediately computed.

Furthermore, even when an action is called, the results aren't automatically persisted by Spark to disk. This differs from other backends, which execute SQL as a `create table` statement, meaning that the result is automatically saved.

This interferes with caching, because Splink assumes that when the the function [`_execute_sql_against_backend()`](https://github.com/moj-analytical-services/splink/blob/cfd29be07ba709403764215e029f395725548b0f/splink/linker.py#L370) is called, this will be evaluated greedily (immediately evaluated) AND the results will be saved to the 'database'.

Another quirk of Spark is that it chunks work up into tasks. This is relevant for two reasons:

- Tasks can suffer from skew, meaning some take longer than others, which can be bad from a performance point of view.
- The number of tasks and how data is partitioned controls how many files are output when results are saved. Some Splink operations results in a very large number of small files which can take a long time to read and write, relative to the same data stored in fewer files.

Repartitioning can be used to rebalance workloads (reduce skew) and to avoid the 'many small files' problem.

### Spark-specific modifications

The logic for Spark is captured in the implementation of `_execute_sql_against_backend()` in the spark_linker.py.

This has three roles:

- It determines how to save result - using either `persist`, `checkpoint` or saving to `.parquet`, with `.parquet` being the default.
- It determines which results to save. Some small results such `__splink__m_u_counts` are immediately converted using `toPandas()` rather than being saved. This is because saving to disk and reloading is expensive and unnecessary.
- It chooses which Spark dataframes to repartition to reduce the number of files which are written/read

Note that repartitioning and saving is independent. Some dataframes are saved without repartitioning. Some dataframes are repartitioned without being saved.

## Caching and pipelining in Spark

This article assumes you've read the [general guide to caching and pipelining](https://moj-analytical-services.github.io/splink/dev_guides/caching.html).

In Spark, some additions have to be made to this general pattern because [all transformation in Spark are lazy](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations).

That is, when we call `df = spark.sql(sql)`, the `df` is not immediately computed.

This interferes with caching, because Splink assumes that when the general function [`_execute_sql()`](https://github.com/moj-analytical-services/splink/blob/cfd29be07ba709403764215e029f395725548b0f/splink/linker.py#L370) is called, this will be evaluted greedily (immediately evaluated).

Another quirk of Spark is that when results are materialised, the number of files produced corresponds to the number of partitions. This can introduce inefficiencies especially for small files, whereby if you're not careful you can end up with hundreds of tiny files, which take a long time to read and write.

### Spark-specific modifications

The logic for Spark is captured in the implementation of [`_execute_sql`](https://github.com/moj-analytical-services/splink/blob/cfd29be07ba709403764215e029f395725548b0f/splink/spark/spark_linker.py#L231) in the SparkLinker(https://github.com/moj-analytical-services/splink/blob/88b526007aeef96f9a407ce940af65f9df9679a1/splink/spark/spark_linker.py), and specifically in the [\_break_lineage](https://github.com/moj-analytical-services/splink/blob/cfd29be07ba709403764215e029f395725548b0f/splink/spark/spark_linker.py#L168) function.

This has two roles:

- It determines how to materialise a result - using either `persist`, `checkpoint` or saving to `.parquet`
- It chooses how to repartition data to reduce the number of files which are written/read

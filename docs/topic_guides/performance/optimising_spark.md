---
tags:
  - Performance
  - Spark
  - Salting
  - Parallelism
---

## Optimising Spark jobs

This topic guide describes how to configure Spark to optimise performance - especially large linkage jobs which are slow or are not completing using default settings.

It is assumed readers have already read the more general [guide to linking big data](./drivers_of_performance.md), and blocking rules are proportionate to the size of the Spark cluster. As a _very_ rough guide, on a small cluster of (say) 8 machines, we recommend starting with blocking rules that generate around 100 million comparisons. Once this is working, loosening the blocking rules to around 1 billion comparisons or more is often achievable.

## Summary:

- Ensure blocking rules are not generating too many comparisons.
- We recommend setting the `break_lineage_method` to `"parquet"`, which is the default
- `num_partitions_on_repartition` should be set so that each file in the output of `predict()` is roughly 100MB.
- Try setting `spark.default.parallelism` to around 5x the number of CPUs in your cluster

For a cluster with 10 CPUs, that outputs about 8GB of data in parquet format, the following setup may be appropriate:

```python
from splink import SparkAPI

spark.conf.set("spark.default.parallelism", "50")
spark.conf.set("spark.sql.shuffle.partitions", "50")

db_api = SparkAPI(
    spark_session=spark,
    break_lineage_method="parquet",
    num_partitions_on_repartition=80,
)
```

## Breaking lineage

Splink uses an iterative algorithm for model training, and more generally, lineage is long and complex. We have found that big jobs fail to complete without further optimisation. This is a [well-known problem](https://www.pdl.cmu.edu/PDL-FTP/Storage/CMU-PDL-18-101.pdf):

!!! quote
    "This long lineage bottleneck is widely known by sophisticated Spark application programmers. A common practice for dealing with long lineage is to have the application program strategically checkpoint RDDs at code locations that truncate much of the lineage for checkpointed data and resume computation immediately from the checkpoint."

Splink will automatically break lineage in sensible places. We have found in practice that, when running Spark jobs backed by AWS S3, the fastest method of breaking lineage is persisting outputs to `.parquet` file.

You can do this using the `break_lineage_method` parameter as follows:

```python
from splink import SparkAPI

db_api = SparkAPI(
    spark_session=spark,
    break_lineage_method="parquet",
    num_partitions_on_repartition=80,
)
```

Other options are `checkpoint` and `persist`, plus a [few others](https://github.com/moj-analytical-services/splink/blob/2ed9f8bf2a21fffafa14e3bb848aa69370043e33/splink/internals/spark/database_api.py#L34) for databricks. For different Spark setups, particularly if you have fast local storage, you may find these options perform better.

## Spark Parallelism

We suggest setting default parallelism to roughly 5x the number of CPUs in your cluster. This is a very rough rule of thumb, and if you're encountering performance problems you may wish to experiment with different values.

One way to set default parallelism is as follows:

```python
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

conf.set("spark.default.parallelism", "50")
conf.set("spark.sql.shuffle.partitions", "50")

sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
```

In general, increasing parallelism will make Spark 'chunk' your job into a larger amount of smaller tasks. This may solve memory issues. But note there is a tradeoff here: if you increase parallelism too high, Spark may take too much time scheduling large numbers of tasks, and may even run out of memory performing this work. See [here](https://stackoverflow.com/a/58251799/1779128). Also note that when blocking, jobs cannot be split into a large number of tasks than the cardinality of the blocking rule. For example, if you block on month of birth, this will be split into 12 tasks, irrespective of the parallelism setting. See [here](https://stackoverflow.com/questions/61073551/increase-parallelism-of-reading-a-parquet-file-spark-optimize-self-join/61077643#61077643). You can use salting (below) to partially address this limitation.

## Repartition after blocking

For some jobs, setting `repartition_after_blocking=True` when you initialise the `SparkAPI` may improve performance.

## Salting

For very large jobs, you may find that [salting your blocking keys](./salting.md) results in faster run times.

## General Spark config

Splink generates large numbers of record comparisons from relatively small input datasets. This is an unusual type of workload, and so default Spark parameters are not always appropriate. Some of the issues encountered are similar to performance issues encountered with Cartesian joins - so some of the tips in [relevant articles](https://www.google.com/search?q=optimising+cartesian+join+spark) may help.

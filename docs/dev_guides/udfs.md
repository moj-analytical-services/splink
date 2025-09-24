# User Defined Functions

User Defined Functions (UDFs) are functions that can be created to add functionality to a given [SQL backend](../topic_guides/splink_fundamentals/backends/backends.md) that does not already exist. These are particularly useful within Splink as it supports multiple SQL engines each with different inherent functionality. UDFs are an important tool for creating consistent functionality across backends.

For example, DuckDB has an in-built string comparison function for [Jaccard similarity](https://duckdb.org/docs/sql/functions/char.html#text-similarity-functions) whereas Spark SQL doesn't have [an equivalent function](https://spark.apache.org/docs/latest/api/sql/index.html). Therefore, a UDF is required to use functions like [`JaccardAtThresholds()`](../api_docs/comparison_library.md#splink.comparison_library.JaccardAtThresholds) and [`JaccardLevel()`](../api_docs/comparison_level_library.md#splink.comparison_level_library.JaccardLevel) with a Spark backend.

## :simple-apachespark: Spark

Spark supports [UDFs written in Scala and Java](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html#:~:text=User%2DDefined%20Functions%20(UDFs),invoke%20them%20in%20Spark%20SQL.).

Splink currently uses UDFs written in Scala and are implemented as follows:

-  The UDFs are created in a separate repository, [`splink_scalaudfs`](https://github.com/moj-analytical-services/splink_scalaudfs), with the Scala functions being defined in [`Similarity.scala`](https://github.com/moj-analytical-services/splink_scalaudfs/blob/main/src/main/scala/uk/gov/moj/dash/linkage/Similarity.scala).
- The functions are then stored in a Java Archive (JAR) file - for more on JAR files, see the [Java documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jarGuide.html).
- Once the JAR file containing the UDFs has been created, it is copied across to the [spark_jars folder](https://github.com/moj-analytical-services/splink/tree/master/splink/files/spark_jars) in Splink.
- Specify the the correct [jar location](https://github.com/moj-analytical-services/splink/blob/master/splink/spark/jar_location.py) within Splink.
- UDFS are then [registered within the Spark Linker](https://github.com/moj-analytical-services/splink/blob/879a34a6f8e548f14733924092f0c773d6f93f72/splink/spark/spark_linker.py#L246).

Now the Spark UDFs have been successfully registered, they can be used in Spark SQL. For example,

```
jaccard("name_column_1", "name_column_2") >= 0.9
```

which provides the basis for functions such as [`JaccardAtThresholds()`](../api_docs/comparison_library.md#splink.comparison_library.JaccardAtThresholds) and [`JaccardLevel()`](../api_docs/comparison_level_library.md#splink.comparison_level_library.JaccardLevel).

## :simple-duckdb: DuckDB

Python UDFs can be registered to a DuckDB connection from version 0.8.0 onwards.

The documentation is [here](https://duckdb.org/docs/api/python/reference/#duckdb.DuckDBPyConnection.create_function), an examples are [here](https://github.com/duckdb/duckdb/pull/7171).  Note that these functions should be registered against the DuckDB connection provided to the linker using `connection.create_function`.

Note that performance will generally be substantially slower than using native DuckDB functions.  Consider using vectorised UDFs were possible - see [here](https://github.com/duckdb/duckdb/pull/7171).

## :simple-amazonaws: Athena

Athena supports [UDFs written in Java](https://docs.aws.amazon.com/athena/latest/ug/querying-udf.html), however these have not yet been implemented in Splink.

## :simple-sqlite: SQLite

Python UDFs can be registered to a SQLite connection using the [`create_function` function](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.create_function).  An example is as follows:

```
from rapidfuzz.distance.Levenshtein import distance
conn = sqlite3.connect(":memory:")
conn.create_function("levenshtein", 2, distance)
```

The function `levenshtein` is now available to use as a Python function


---
tags:
  - Spark
  - DuckDB
  - Athena
  - SQLite
  - Postgres
  - Backends
---

# Splink's SQL backends: Spark, DuckDB, etc

Splink is a Python library. However, it implements all data linking computations by generating SQL, and submitting the SQL statements to a backend of the user's choosing for execution.

The Splink code you write is almost identical between backends, so it's straightforward to migrate between backends. Often, it's a good idea to start working using DuckDB on a sample of data, because it will produce results very quickly. When you're comfortable with your model, you may wish to migrate to a big data backend to estimate/predict on the full dataset.

## Choosing a backend

### Considerations when choosing a SQL backend for Splink

When choosing which backend to use when getting started with Splink, there are a number of factors to consider:

- the size of the dataset(s)
- the amount of boilerplate code/configuration required
- access to specific (sometimes proprietary) platforms
- the backend-specific features offered by Splink
- the level of support and active development offered by Splink

Below is a short summary of each of the backends available in Splink.

### :simple-duckdb: DuckDB

DuckDB is recommended for most users for all but the largest linkages.

It is the fastest backend, and is capable of linking large datasets, especially if you have access to high-spec machines.

As a rough guide it can:

- Link up to around 5 million records on a modern laptop (4 core/16GB RAM)
- Link tens of millions of records on high spec cloud computers very fast.

For further details, see the results of formal benchmarking [here](https://www.robinlinacre.com/fast_deduplication/).

DuckDB is also recommended because for many users its simplest to set up.

It can be run on any device with python installed and it is installed automatically with Splink via `pip install splink`. DuckDB has complete coverage for the functions in the Splink [comparison libraries](../../../api_docs/comparison_level_library.md).  Alongside the Spark linker, it receives most attention from the development team.

See the DuckDB [deduplication example notebook](../../../demos/examples/duckdb/deduplicate_50k_synthetic.ipynb) to get a better idea of how Splink works with DuckDB.


### :simple-apachespark: Spark

Spark is recommended for:

- Very large linkages, especially where DuckDB is performing poorly or running out of memory, or
- Or have easier access to a Spark cluster than a single high-spec instance to run DuckDB

It is not our default recommendation for most users because:

- It involves more configuration than users, such as registering UDFs and setting up a Spark cluster
- It is slower than DuckDB for many

The Spark linker has complete coverage for the functions in the Splink [comparison libraries](../../../api_docs/comparison_level_library.md).

If working with Databricks note that the Splink development team does not have access to a Databricks environment so we can struggle help DataBricks-specific issues.

See the Spark [deduplication example notebook](../../../demos/examples/spark/deduplicate_1k_synthetic.ipynb) for an example of how Splink works with Spark.

### :simple-amazonaws: Athena

Athena is a big data SQL backend provided on AWS which is great for large datasets (10+ million records). It requires access to a live AWS account and as a persistent database, requires some additional management of the tables created by Splink. Athena has reasonable, but not complete, coverage for fuzzy matching functions, see [Presto](https://prestodb.io/docs/current/functions/string.html). At this time, the Athena backend is being used sparingly by the Splink development team so receives minimal levels of support.

In addition, from a development perspective, the necessity for an AWS connection makes testing Athena code more difficult, so there may be occasional bugs that would normally be caught by our testing framework.

See the Athena [deduplication example notebook](../../../demos/examples/athena/deduplicate_50k_synthetic.ipynb) to get a better idea of how Splink works with Athena.

### :simple-sqlite: SQLite

SQLite is similar to DuckDB in that it is, generally, more suited to smaller datasets. SQLite is simple to setup and can be run directly in a Jupyter notebook, but is not as performant as DuckDB. SQLite has reasonable, but not complete, coverage for the functions in the Splink [comparison libraries](../../../api_docs/comparison_level_library.md), with gaps in array and date comparisons. String fuzzy matching, while not native to SQLite is available via python UDFs which has some [performance implications](#additional-information-for-specific-backends). SQLite is not actively been used by the Splink team so receives minimal levels of support.

### :simple-postgresql: PostgreSQL

PostgreSQL is a relatively new linker, so we have not fully tested performance or what size of datasets can processed with Splink. The Postgres backend requires a Postgres database, so it is recommend to use this backend only if you are working with a pre-existing Postgres database. Postgres has reasonable, but not complete, coverage for the functions in the Splink [comparison libraries](../../../api_docs/comparison_level_library.md), with gaps in string fuzzy matching functionality due to the lack of some string functions in Postgres. At this time, the Postgres backend is not being actively used by the Splink development team so receives minimal levels of support.

More details on using Postgres as a Splink backend can be found on the [Postgres page](./postgres.md).

## Using your chosen backend

Choose the relevant DBAPI:

Once you have initialised the `linker` object, there is no difference in the subsequent code between backends.

=== ":simple-duckdb: DuckDB"

    ```python
    from splink import Linker, DuckDBAPI

    linker = Linker(df, settings, db_api=DuckDBAPI(...))
    ```

=== ":simple-apachespark: Spark"

    ```python
    from splink import Linker, SparkAPI

    linker = Linker(df, settings, db_api=SparkAPI(...))
    ```

=== ":simple-amazonaws: Athena"

    ```python
    from splink import Linker, AthenaAPI

    linker = Linker(df, settings, db_api=AthenaAPI(...))
    ```

=== ":simple-sqlite: SQLite"

    ```python
    from splink import Linker, SQLiteAPI

    linker = Linker(df, settings, db_api=SQLiteAPI(...))

    ```

=== ":simple-postgresql: PostgreSQL"

    ```python
    from splink import Linker, PostgresAPI

    linker = Linker(df, settings, db_api=PostgresAPI(...))

    ```

## Additional Information for specific backends

### :simple-sqlite: SQLite

[**SQLite**](https://www.sqlite.org/index.html) does not have native support for [fuzzy string-matching](../../comparisons/comparators.md) functions.
However, the following are available for Splink users as python [user-defined functions (UDFs)](../../../dev_guides/udfs.md#sqlite)  which are automatically registered when calling `SQLiteAPI()`

* `levenshtein`
* `damerau_levenshtein`
* `jaro`
* `jaro_winkler`

However, there are a couple of points to note:

* These functions are implemented using the [RapidFuzz](https://maxbachmann.github.io/RapidFuzz/) package, which must be installed if you wish to make use of them, via e.g. `pip install rapidfuzz`. If you do not wish to do so you can disable the use of these functions when creating your linker:
```py
SQLiteAPI(register_udfs=False)
```
* As these functions are implemented in python they will be considerably slower than any native-SQL comparisons. If you find that your model-training or predictions are taking a large time to run, you may wish to consider instead switching to DuckDB (or some other backend).

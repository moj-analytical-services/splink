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

Splink is a Python library. It implements all data linking computations by generating SQL, and submitting the SQL statements to a backend of the user's choosing for execution.

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

DuckDB is recommended for most users. It is the fastest backend, and is capable of linking large datasets, especially if you have access to high-spec machines.

As a rough guide it can:

- Link up to around 5 million records on a modern laptop (4 core/16GB RAM)
- Link tens of millions of records on high spec cloud computers very fast.

For further details, see the results of formal benchmarking [here](https://www.robinlinacre.com/fast_deduplication/).

DuckDB is also recommended because for many users its simplest to set up.

It can be run on any device with python installed and it is installed automatically with Splink via `pip install splink`. DuckDB has complete coverage for the functions in the Splink [comparison libraries](../../../comparison_level_library.md) and, as a mainstay of the Splink development team, is actively maintained with features being added regularly.

See the DuckDB [deduplication example notebook](../../../demos/examples/duckdb/deduplicate_50k_synthetic.ipynb) to get a better idea of how Splink works with DuckDB.

### :simple-apachespark: Spark

Spark is a system for distributed computing which is great for large datasets. It is more involved in terms of configuration, with more boilerplate code than the likes of DuckDB. Spark has complete coverage for the functions in the Splink [comparison libraries](../../../comparison_level_library.md) and, as a mainstay of the Splink development team, is actively maintained with features being added regularly.

Spark is primarily recommended for users who either:

- Need to link enormous datasets (100 million records+), and have experience out of memory/out of disk problems with DuckDB
- Or have easier access to a Spark cluster than a single high-spec instance to run DuckDB

If working with Databricks note that the Splink development team does not have access to a Databricks environment there will be instances where we will be unable to provide support.

See the Spark [deduplication example notebook](../../../demos/examples/spark/deduplicate_1k_synthetic.ipynb) to get a better idea of how Splink works with Spark.

### :simple-amazonaws: Athena

Athena is a big data SQL backend provided on AWS which is great for large datasets (10+ million records). It requires access to a live AWS account and as a persistent database, requires some additional management of the tables created by Splink. Athena has reasonable, but not complete, coverage for the functions in the Splink [comparison libraries](../../../comparison_level_library.md), with gaps in string fuzzy matching functionality due to the lack of some string functions in Athena's underlying SQL engine, [Presto](https://prestodb.io/docs/current/). At this time, the Athena backend is being used sparingly by the Splink development team so receives minimal levels of support.

In addition, from a development perspective, the necessity for an AWS connection makes testing Athena code more difficult, so there may be occasional bugs that would normally be caught by our testing framework.

See the Athena [deduplication example notebook](../../../demos/examples/athena/deduplicate_50k_synthetic.ipynb) to get a better idea of how Splink works with Athena.

### :simple-sqlite: SQLite

SQLite is similar to DuckDB in that it is, generally, more suited to smaller datasets. SQLite is simple to setup and can be run directly in a Jupyter notebook, but is not as performant as DuckDB. SQLite has reasonable, but not complete, coverage for the functions in the Splink [comparison libraries](../../../comparison_level_library.md), with gaps in array and date comparisons. String fuzzy matching, while not native to SQLite is available via python UDFs which has some [performance implications](#additional-information-for-specific-backends). SQLite is not actively been used by the Splink team so receives minimal levels of support.

### :simple-postgresql: PostgreSql

PostgreSql is a relatively new linker, so we have not fully tested performance or what size of datasets can processed with Splink. The Postgres backend requires a Postgres database, so it is recommend to use this backend only if you are working with a pre-existing Postgres database. Postgres has reasonable, but not complete, coverage for the functions in the Splink [comparison libraries](../../../comparison_level_library.md), with gaps in string fuzzy matching functionality due to the lack of some string functions in Postgres. At this time, the Postgres backend is not being actively used by the Splink development team so receives minimal levels of support.

More details on using Postgres as a Splink backend can be found on the [postgres page](./postgres.md).

## Using your chosen backend

Import the linker from the backend of your choosing, and the backend-specific comparison libraries.

Once you have initialised the `linker` object, there is no difference in the subsequent code between backends.

=== ":simple-duckdb: DuckDB"

    ```python
    from splink.duckdb.linker import DuckDBLinker
    import splink.duckdb.comparison_library as cl
    import splink.duckdb.comparison_level_library as cll

    linker = DuckDBLinker(your_args)
    ```

=== ":simple-apachespark: Spark"

    ```python
    from splink.spark.linker import SparkLinker
    import splink.spark.comparison_library as cl
    import splink.spark.comparison_level_library as cll

    linker = SparkLinker(your_args)
    ```

=== ":simple-amazonaws: Athena"

    ```python
    from splink.athena.linker import AthenaLinker
    import splink.athena.comparison_library as cl
    import splink.athena.comparison_level_library as cll

    linker = AthenaLinker(your_args)
    ```

=== ":simple-sqlite: SQLite"

    ```python
    from splink.sqlite.linker import SQLiteLinker
    import splink.sqlite.comparison_library as cl
    import splink.sqlite.comparison_level_library as cll

    linker = SQLiteLinker(your_args)

    ```

=== ":simple-postgresql: PostgreSql"

    ```python
    from splink.postgres.linker import PostgresLinker
    import splink.postgres.comparison_library as cl
    import splink.postgres.comparison_level_library as cll

    linker = PostgresLinker(your_args)

    ```

## Additional Information for specific backends

### :simple-sqlite: SQLite

[**SQLite**](https://www.sqlite.org/index.html) does not have native support for [fuzzy string-matching](../../comparisons/comparators.md) functions.
However, some are available for Splink users as python [user-defined functions (UDFs)](../../../dev_guides/udfs.html#sqlite):

* [`levenshtein`](../../../comparison_level_library.html#splink.comparison_level_library.LevenshteinLevelBase)
* [`damerau_levenshtein`](../../../comparison_level_library.html#splink.comparison_level_library.DamerauLevenshteinLevelBase)
* [`jaro`](../../../comparison_level_library.html#splink.comparison_level_libraryJaroLevelBase)
* [`jaro_winkler`](../../../comparison_level_library.html#splink.comparison_level_library.JaroWinklerLevelBase)

However, there are a couple of points to note:

* These functions are implemented using the [rapidfuzz](https://maxbachmann.github.io/RapidFuzz/) package, which must be installed if you wish to make use of them, via e.g. `pip install rapidfuzz`. If you do not wish to do so you can disable the use of these functions when creating your linker:
```py
linker = SQLiteLinker(df, settings, ..., register_udfs=False)
```
* As these functions are implemented in python they will be considerably slower than any native-SQL comparisons. If you find that your model-training or predictions are taking a large time to run, you may wish to consider instead switching to DuckDB (or some other backend).
---
tags:
  - Dedupe
  - Link
  - Link and Dedupe
---

# Link type: Linking, Deduping or Both

Splink allows data to be linked, deduplicated or both.

Linking refers to finding links between datasets, whereas deduplication finding links within datasets.

Data linking is therefore only meaningful when more than one dataset is provided.

This guide shows how to specify the settings dictionary and initialise the linker for the three link types.

## Deduplication

The `dedupe_only` link type expects the user to provide a single input table, and is specified as follows

=== ":simple-duckdb: DuckDB"
    ``` python
    from splink.duckdb.linker import DuckDBLinker

    settings = {
        "link_type": "dedupe_only",
        # etc.
    }
    linker = DuckDBLinker(df, settings)
    ```
=== ":simple-apachespark: Spark"
    ``` python
    from splink.spark.linker import SparkLinker

    settings = {
        "link_type": "dedupe_only",
        # etc.
    }
    linker = SparkLinker(df, settings)
    ```
=== ":simple-amazonaws: Athena"
    ``` python
    from splink.athena.linker import AthenaLinker

    settings = {
        "link_type": "dedupe_only",
        # etc.
    }
    linker = AthenaLinker(df, settings)
    ```
=== ":simple-sqlite: SQLite"
    ``` python
    from splink.sqlite.linker import SQLiteLinker

    settings = {
        "link_type": "dedupe_only",
        # etc.
    }
    linker = SQLiteLinker(df, settings)
    ```

## Link only

The `link_only` link type expects the user to provide a list of input tables, and is specified as follows:

=== ":simple-duckdb: DuckDB"
    ``` python
    from splink.duckdb.linker import DuckDBLinker

    settings = {
        "link_type": "link_only",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = DuckDBLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-apachespark: Spark"
    ``` python
    from splink.spark.linker import SparkLinker

    settings = {
        "link_type": "link_only",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = SparkLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-amazonaws: Athena"
    ``` python
    from splink.athena.linker import AthenaLinker

    settings = {
        "link_type": "link_only",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = AthenaLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-sqlite: SQLite"
    ``` python
    from splink.sqlite.linker import SQLiteLinker

    settings = {
        "link_type": "link_only",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = SQLiteLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```

The `input_table_aliases` argument is optional and are used to label the tables in the outputs. If not provided, defaults will be automatically chosen by Splink.

## Link and dedupe

The `link_and_dedupe` link type expects the user to provide a list of input tables, and is specified as follows:

=== ":simple-duckdb: DuckDB"
    ``` python
    from splink.duckdb.linker import DuckDBLinker

    settings = {
        "link_type": "link_and_dedupe",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = DuckDBLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-apachespark: Spark"
    ``` python
    from splink.spark.linker import SparkLinker

    settings = {
        "link_type": "link_and_dedupe",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = SparkLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-amazonaws: Athena"
    ``` python
    from splink.athena.linker import AthenaLinker

    settings = {
        "link_type": "link_and_dedupe",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = AthenaLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```
=== ":simple-sqlite: SQLite"
    ``` python
    from splink.sqlite.linker import SQLiteLinker

    settings = {
        "link_type": "link_and_dedupe",
        # etc.
        }

    input_aliases = ["table_1", "table_2", "table_3"]
    linker = SQLiteLinker([df_1, df_2, df_3], settings, input_table_aliases=input_aliases)
    ```

The `input_table_aliases` argument is optional and are used to label the tables in the outputs. If not provided, defaults will be automatically chosen by Splink.

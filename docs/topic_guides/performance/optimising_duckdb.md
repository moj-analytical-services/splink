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

---
tags:
  - SQL
  - Data Frames
  - SplinkDataFrame
toc_depth: 2
---
# Retrieving and Querying Splink Results

When Splink returns results, it does so in the format of a `SplinkDataFrame`.   This is needed to allow Splink to provide results in a uniform format across the different [database backends](../splink_fundamentals/backends/backends.md)

For example, when you run `df_predict = linker.inference.predict()`, the result `df_predict` is a `SplinkDataFrame`.

A `SplinkDataFrame` is an abstraction of a table in the underlying backend database, and provides several convenience methods for interacting with the underlying table.
For detailed information check [the full API](../../api_docs/splink_dataframe.md).

### Converting to other types

You can convert a `SplinkDataFrame` into a variety of types.  You will usually get best performance by using the backends native types e.g.:

- For DuckDB, use `splink_df.as_duckdbpyrelation()` to obtain a `DuckDBPyRelation`
- For Spark, use `splink_df.as_spark_dataframe()` to obtain a Spark `

You can also convert a `SplinkDataFrame` into other types, including:

- a [PyArrow](https://arrow.apache.org/docs/python/) Table, using `splink_df.as_pyarrow_table()`
- a Pandas dataframe, using `splink_df.as_pandas_dataframe()`
- a list of record dictionaries, using `splink_df.as_record_list()`


For large linkages, it is not recommended to convert the whole `SplinkDataFrame` into an in-memory type because Splink results can be very large, so doing so can be slow and result in out of memory errors. Usually it will be better to use [SQL to query the tables directly](#querying-tables).



### Querying tables

You can run SQL queries directly against a `SplinkDataFrame` using its `query_sql` method, referring to the table within your SQL as `{this}`. This is the recommended approach as it's typically faster and more memory efficient than using pandas dataframes. If you need the underlying table name directly, you can find it using `splink_df.physical_name`.

The following is an example of this approach, in which we use SQL to find the best match to each input record in a `link_type="link_only"` job (i.e remove duplicate _matches_):

```python
# linker is a Linker with link_type set to "link_only"
df_predict = linker.inference.predict(threshold_match_probability=0.75)

sql = """
with ranked as
(
select *,
row_number() OVER (
    PARTITION BY unique_id_l order by match_weight desc
    ) as row_number
from {this}
)

select *
from ranked
where row_number = 1
"""

df_query_result = df_predict.query_sql(sql)  # SplinkDataFrame
```

`query_sql` returns a `SplinkDataFrame`. If you want the results as a pandas dataframe, convert it as follows:
```python
df_query_result = df_predict.query_sql(sql).as_pandas_dataframe()
```

### Saving results

If you have a `SplinkDataFrame`, you may wish to store the results in some file _outside_ of your database.
As tables may be large, there are a couple of convenience methods for doing this directly without needing to load the table into memory.
Currently Splink supports saving frames to either `csv` or `parquet` format.
Of these we generally recommend the latter, as it is typed, compressed, column-oriented, and easily supports nested data.

To save results, simply use the methods [`to_csv()`](../../api_docs/splink_dataframe.md) or [`to_parquet()`](../../api_docs/splink_dataframe.md) - for example:
```python
df_predict = linker.inference.predict()
df_predict.to_parquet("splink_predictions.parquet", overwrite=True)
# or alternatively:
df_predict.to_csv("splink_predictions.csv", overwrite=True)
```

### Creating a `SplinkDataFrame`


You can  create a `SplinkDataFrame` for any table in your database. You will need to already have a `linker` to manage interactions with the database:
```python
import duckdb
import pyarrow as pa

from splink import Linker, SettingsCreator, DuckDBAPI
from splink.datasets import splink_datasets

con = duckdb.connect()
arrow_numbers = pa.Table.from_pydict({"id": [1, 2, 3], "number": ["one", "two", "three"]})
con.sql("CREATE TABLE number_table AS SELECT * FROM arrow_numbers")

db_api = DuckDBAPI(connection=con)
df = db_api.register(splink_datasets.fake_1000, dataset_display_name="fake_1000")

linker = Linker(df, settings=SettingsCreator(link_type="dedupe_only"))
splink_df = linker.table_management.register_table("number_table", "a_templated_name")
splink_df.as_pandas_dataframe()
```
```


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

You can convert a `SplinkDataFrame` into a Pandas dataframe using `splink_df.as_pandas_dataframe()`.

To view the first few records use a limit statement: `splink_df.as_pandas_dataframe(limit=10)`.

For large linkages, it is not recommended to convert the whole `SplinkDataFrame` to pandas because Splink results can be very large, so converting them into pandas can be slow and result in out of memory errors. Usually it will be better to use [SQL to query the tables directly](#querying-tables).



### Querying tables

You can find out the name of the table in the underlying database using `splink_df.physical_name`. This enables you to run SQL queries directly against the results.
You can execute queries using `linker.misc.query_sql` -
this is the recommended approach as it's typically faster and more memory efficient than using pandas dataframes.

The following is an example of this approach, in which we use SQL to find the best match to each input record in a `link_type="link_only"` job (i.e remove duplicate _matches_):

```python
# linker is a Linker with link_type set to "link_only"
df_predict = linker.inference.predict(threshold_match_probability=0.75)

sql = f"""
with ranked as
(
select *,
row_number() OVER (
    PARTITION BY unique_id_l order by match_weight desc
    ) as row_number
from {df_predict.physical_name}
)

select *
from ranked
where row_number = 1
"""

df_query_result = linker.misc.query_sql(sql)  # pandas dataframe
```

Note that `linker.misc.query_sql` will return a pandas dataframe by default, but you can instead return a `SplinkDataFrame` as follows:
```python
df_query_result = linker.misc.query_sql(sql, output_type='splink_df')
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
import pandas as pd
import duckdb

from splink import Linker, SettingsCreator, DuckDBAPI
from splink.datasets import splink_datasets

con = duckdb.connect()
df_numbers = pd.DataFrame({"id": [1, 2, 3], "number": ["one", "two", "three"]})
con.sql("CREATE TABLE number_table AS SELECT * FROM df_numbers")

db_api = DuckDBAPI(connection=con)
df = splink_datasets.fake_1000

linker = Linker(df, settings=SettingsCreator(link_type="dedupe_only"), db_api=db_api)
splink_df = linker.table_management.register_table("number_table", "a_templated_name")
splink_df.as_pandas_dataframe()
```
```


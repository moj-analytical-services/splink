---
tags:
  - SQL
  - Data Frames
  - SplinkDataFrame
toc_depth: 2
---
# Retrieving and Querying Splink Results

Once you have [created your linkage model](./settings.md), [trained your parameters](../demos/tutorials/04_Estimating_model_parameters.html) and [predicted your match probabilities](../demos/tutorials/05_Predicting_results.html) you may want to run queries on your results. Splink provides several methods for interacting with the tables produced in this process.

## SplinkDataFrame

Splink returns tables of results using a class called a `SplinkDataFrame`.
For example, when you run `df_predict = linker.predict()`, the result `df_predict` is a `SplinkDataFrame`.

A `SplinkDataFrame` is an abstraction of a table in the underlying backend database, and provides several convenience methods for interacting with the underlying table.
For detailed information check [the full API](../SplinkDataFrame.md).

### Converting to other types

It's possible to convert a `SplinkDataFrame` into a Pandas dataframe using `splink_df.as_pandas_dataframe()`. However, this is not recommended because Splink results can be very large, so converting them into pandas can be slow and result in out of memory errors. Usually it will be better to use [SQL to query the tables directly](#querying-tables).

Alternatively you can use `splink_df.as_record_dict()` to get the table as a list of dictionary records - similarly this is not recommended aside from sufficiently small tables, as it involves loading the full data set into memory.

### Querying tables

You can find out the name of the table in the underlying database using `splink_df.physical_name`. This enables you to run SQL queries directly against the results.
You can execute queries using `linker.query_sql` - 
this is the recommended approach as it's typically faster and more memory efficient than using pandas dataframes.

The following is an example of this approach, in which we use SQL to find the best match to each input record in a `link_type="link_only"` job (i.e remove duplicate matches):

```python
# linker is a Linker with link_type set to "link_only"
results = linker.predict(threshold_match_probability=0.75)

sql = f"""
with ranked as
(
select *,
row_number() OVER (
    PARTITION BY unique_id_l order by match_weight desc
    ) as row_number
from {results.physical_name}
)

select *
from ranked
where row_number = 1
"""

df_query_result = linker.query_sql(sql)  # pandas dataframe
```

Note that `linker.query_sql` will return a pandas dataframe by default, but you can instead return a `SplinkDataFrame` as follows:
```python
df_query_result = linker.query_sql(sql, output_type='splink_df')
```

### Saving results

If you have a `SplinkDataFrame`, you may wish to store the results in some file _outside_ of your database.
As tables may be large, there are a couple of convenience methods for doing this directly without needing to load the table into memory.
Currently Splink supports saving frames to either `csv` or `parquet` format.
Of these we generally recommend the latter, as it is typed, compressed, column-oriented, and easily supports nested data.

To save results, simply use the methods [`to_csv()`](../SplinkDataFrame.md#splink.splink_dataframe.SplinkDataFrame.to_csv) or [`to_parquet()`](../SplinkDataFrame.md#splink.splink_dataframe.SplinkDataFrame.to_parquet) - for example:
```python
df_predict = linker.predict()
df_predict.to_parquet("splink_predictions.parquet", overwrite=True)
# or alternatively:
df_predict.to_csv("splink_predictions.csv", overwrite=True)
```

### Creating a `SplinkDataFrame`

Generally speaking, any Splink method that results in a table will return a `SplinkDataFrame`. This includes:

* [`linker.predict()`](../linker.md#splink.linker.Linker.predict)
* [`linker.cluster_pairwise_predictions_at_threshold()`](../linker.md#splink.linker.Linker.cluster_pairwise_predictions_at_threshold)
* [`linker.find_matches_to_new_records()`](../linker.md#splink.linker.Linker.find_matches_to_new_records)
* [`linker.compare_two_records()`](../linker.md#splink.linker.Linker.compare_two_records)
* [`linker.count_num_comparisons_from_blocking_rules_for_prediction()`](../linker.md#count_num_comparisons_from_blocking_rules_for_prediction)
* [`linker.truth_space_table_from_labels_table()`](../linker.md#splink.linker.Linker.truth_space_table_from_labels_table)
* [`linker.prediction_errors_from_labels_table()`](../linker.md#splink.linker.Linker.prediction_errors_from_labels_table)
* [`linker.compute_tf_table()`](../linker.md#splink.linker.Linker.compute_tf_table)
* [`linker.deterministic_link()`](../linker.md#splink.linker.Linker.deterministic_link)

Aside from these you can create a `SplinkDataFrame` for any table in your database. You will need to already have a [linker](../linker.md)
to manage interactions with the database:
```python
import pandas as pd
import duckdb

con = duckdb.connect()
df_numbers = pd.DataFrame({"id": [1, 2, 3], "number": ["one", "two", "three"]})
con.sql("CREATE TABLE number_table AS SELECT * FROM df_numbers")

from splink.duckdb.linker import DuckDBLinker, DuckDBDataFrame
from splink.datasets import splink_datasets

df = splink_datasets.fake_1000

linker = DuckDBLinker(df, {"link_type": "dedupe_only"}, connection=con)

my_splink_df = DuckDBDataFrame("a_templated_name", "number_table", linker)
```

Alternatively if you have an in-memory data source you can use [`linker.register_table()`](../linker.md#splink.linker.Linker.register_table) which will register the table with the database backend and return a `SplinkDataFrame`:
```python
import pandas as pd
from splink.duckdb.linker import DuckDBLinker, DuckDBDataFrame
from splink.datasets import splink_datasets

df = splink_datasets.fake_1000

linker = DuckDBLinker(df, {"link_type": "dedupe_only"})

df_numbers = pd.DataFrame({"id": [1, 2, 3], "number": ["one", "two", "three"]})
splink_df_numbers = linker.register_table(df_numbers, "table_name_in_backend")
```

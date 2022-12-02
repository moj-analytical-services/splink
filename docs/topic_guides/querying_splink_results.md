# Retrieving and Querying Splink results with the SplinkDataFrame

Splink returns tables of results using a class called a `SplinkDataFrame`. e.g. when you run `df_predict = linker.predict()` `df_predict` is a `SplinkDataFrame`

A `SplinkDataFrame` is a abstraction Splink's results, which under the hood are a table in the underlying database.

It's possible to convert a `SplinkDataFrame` into a Pandas dataframe using `splink_df.as_pandas_dataframe()`. However, this is not recommended because Splink results can be very large, so converting them into pandas can be slow and result in out of memory errors.

You can find out the name of the table in the underlying database using `df_predict.physical_name`. This enables you to run SQL queries directly against the results.

You can execute queries using `linker.query_sql`.

This is the recommended approach as it's typically faster and more memory efficient than using pandas dataframes.

The following is an example of this approach, in which we use SQL to find the best match to each input record in a `link_type="link_only"` job (i.e remove duplicate matches):

```python
# Linker is a duckdb linker with link_type set to "link_only"
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
linker.query_sql(sql)
```

Note that `linker.query_sql` will return a pandas dataframe by default, but you can instead return a SplinkDataFrame as follows:

```python
linker.query_sql(sql, output_type='splink_df')
```

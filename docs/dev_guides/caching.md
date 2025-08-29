## Caching and pipelining

Splink is able to run against multiple SQL backends because all of the core data linking calculations are implemented in SQL. This SQL can therefore be submitted to a chosen SQL backend for execution.

Computations in Splink often take the form of a number of `select` statements run in sequence.

For example, the `predict()` step:

- Inputs `__splink__df_concat_with_tf` and outputs `__splink__df_blocked`
- Inputs `__splink__df_blocked` and outputs `__splink__df_comparison_vectors`
- Inputs `__splink__df_comparison_vectors` and outputs `__splink__df_match_weight_parts`
- Inputs `__splink__df_match_weight_parts` and outputs `__splink__df_predict`

To make this run faster, two key optimisations are implemented:

- Pipelining - combining multiple `select` statements into a single statement using `WITH`([CTE](https://www.postgresql.org/docs/current/queries-with.html)) queries
- Caching: saving the results of calculations so they don't need recalculating. This is especially useful because some intermediate calculations are reused multiple times during a typical Splink session

This article discusses the general implementation of caching and pipelining. The implementation needs some alterations for certain backends like Spark, which lazily evaluate SQL by default.

### Implementation: Pipelining

A [`SQLPipeline` class](https://github.com/moj-analytical-services/splink/blob/5f9f1d686e115ce91c9a1e51fe276d254a4deabe/splink/pipeline.py#L38) manages SQL pipelining.

A `SQLPipeline` is composed of a number of [`SQLTask` objects](https://github.com/moj-analytical-services/splink/blob/5f9f1d686e115ce91c9a1e51fe276d254a4deabe/splink/pipeline.py#L10), each of which represents a select statement.

The code is fairly straightforward: Given a sequence of `select` statements, `[a,b,c]` they are combined into a single query as follows:

```
with
a as (a_sql),
b as (b_sql),
c_sql
```

To make this work, each statement (a,b,c) in the pipeline must refer to the previous step by name. For example, `b_sql` probably selects from the `a_sql` table, which has been aliased `a`. So `b_sql` must use the table name `a` to refer to the result of `a_sql`.

To make this tractable, each `SQLTask` has an `output_table_name`. For example, the `output_table_name` for `a_sql` in the above example is `a`.

For instance, in the `predict()` pipeline above, the first `output_table_name` is `__splink__df_blocked`. By giving each task a meaningful `output_table_name`, subsequent tasks can reference previous outputs in a way which is semantically clear.

### Implementation: Caching

When a SQL pipeline is executed, it has two output names:

- A `physical_name`, which is the name of the materialised table in the output database e.g. `__splink__df_predict_cbc9833`
- A `templated_name`, which is a descriptive name of what the table represents e.g. `__splink__df_predict`

Each time Splink runs a SQL pipeline, the [SQL string is hashed](https://github.com/moj-analytical-services/splink/blob/5f9f1d686e115ce91c9a1e51fe276d254a4deabe/splink/linker.py#L400). This creates a unique identifier for that particular SQL string, which serves to identify the output.

When Splink is asked to execute a SQL string, before execution, it checks whether the resultant table already exists. If it does, it returns the table rather than recomputing it.

For example, when we run `linker.inference.predict()`, Splink:

- Generates the SQL tasks
- Pipelines them into a single SQL statement
- Hashes the statement to create a physical name for the outputs `__splink__df_predict_cbc9833`
- Checks whether a table with physical name `__splink__df_predict_cbc9833` already exists in the database
- If not, executes the SQL statement, creating table `__splink__df_predict_cbc9833` in the database.

In terms of implementation, the following happens:

- SQL statements are generated an put in the queue - see [here](https://github.com/moj-analytical-services/splink/blob/6e978a6a61058a73ef6c49039e0d796b12673c1b/splink/linker.py#L982-L983)
- Once all the tasks have been added to the queue, we call `_execute_sql_pipeline()` see [here](https://github.com/moj-analytical-services/splink/blob/6e978a6a61058a73ef6c49039e0d796b12673c1b/splink/linker.py#L994)
- The SQL is combined into a single pipelined statement [here](https://github.com/moj-analytical-services/splink/blob/6e978a6a61058a73ef6c49039e0d796b12673c1b/splink/linker.py#L339)
- We call `_sql_to_splink_dataframe()` which returns the table (from the cache if it already exists, or it executes the SQL)
- The table is returned as a `SplinkDataframe`, an abstraction over a table in a database. See [the API docs for SplinkDataFrame](../api_docs/splink_dataframe.md).

#### Some cached tables do not need a hash

A hash is required to uniquely identify some outputs. For example, blocking is used in several places in Splink, with _different results_. For example, the `__splink__df_blocked` needed to estimate parameters is different to the `__splink__df_blocked` needed in the `predict()` step.

As a result, we cannot materialise a single table called `__splink__df_blocked` in the database and reuse it multiple times. This is why we append the hash of the SQL, so that we can uniquely identify the different versions of `__splink__df_blocked` which are needed in different contexts.

There are, however, some tables which are globally unique. They only take a single form, and if they exist in the cache they never need recomputing.

An example of this is `__splink__df_concat_with_tf`, which represents the concatenation of the input dataframes.

To create this table, we can execute [`_sql_to_splink_dataframe`](https://github.com/moj-analytical-services/splink/blob/6e978a6a61058a73ef6c49039e0d796b12673c1b/splink/linker.py#L386-L387) with `materialise_as_hash` set to `False`. The resultant materialised table will not have a hash appended, and will simply be called `__splink__df_concat_with_tf`. This is useful, because when performing calculations Splink can now check the cache for `__splink__df_concat_with_tf` each time it is needed.

In fact, many Splink pipelines begin with the assumption that this table exists in the database, because the first `SQLTask` in the pipeline refers to a table named `__splink__df_concat_with_tf`. To ensure this is the case, a [function is used to create this table if it doesn't exist.](https://github.com/moj-analytical-services/splink/blob/6e978a6a61058a73ef6c49039e0d796b12673c1b/splink/linker.py#L980)

### Using pipelining to optimise Splink workloads

At what point should a pipeline of `SQLTask`s be executed (materialised into a physical table)?

For any individual output, it will usually be fastest to pipeline the full linage of tasks, right from raw data through to the end result.

However, there are many intermediate outputs which are used by many different Splink operations.

Performance can therefore be improved by computing and saving these intermediate outputs to a cache, to ensure they don't need to be computed repeatedly.

This is achieved by enqueueing SQL to a pipeline and strategically calling `execute_sql_pipeline` to materialise results that need to cached.

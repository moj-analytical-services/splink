# SQL Transpilation in Splink, and how we support multiple SQL backends

In Splink, all the core data linking algorithms are implemented in SQL. This allows computation to be offloaded to a SQL backend of the users choice.

One difficulty with this paradigm is that SQL implementations differ - the functions available in (say) the Spark dialect of SQL differ from those available in DuckDB SQL. And to make matters worse, functions with the same name may behave differently (e.g. different arguments, arguments in different orders, etc.).

Splink therefore needs a mechanism of writing SQL statements that are able to run against all the target SQL backends (engines).

Details are as follows:

### 1. Core data linking algorithms are Splink

Core data linking algorithms are implemented in 'backend agnostic' SQL. So they're written using basic SQL functions that are common across the available in all the target backends, and don't need any translation.

It has been possible to write all of the core Splink logic in SQL that is consistent between dialects.

However, this is not the case with `Comparisons`, which tend to use backend specific SQL functions like `jaro_winker`, whose function names and signatures differ between backends.

### 2. User-provided SQL is interpolated into these dialect-agnostic SQL statements

The user provides custom SQL is two places in Splink:

1. Blocking rules
2. The `sql_condition` (see [here](../api_docs/settings_dict_guide.md#sql_condition)) provided as part of a `Comparison`

The user is free to write this SQL however they want.

It's up to the user to ensure the SQL they provide will execute successfully in their chosen backend. So the `sql_condition` must use functions that exist in the target execution engine

### 3. Backends can implement transpilation and or dialect steps to further transform the SQL if needed

Occasionally some modifications are needed to the SQL to ensure it executes against the target backend.

`sqlglot` is used for this purpose. For instance, a custom dialect is implemented in the Spark linker.

A transformer is implemented in the Athena linker.

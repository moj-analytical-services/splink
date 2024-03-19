---
tags:
  - Postgres
  - Backends
---

# Using PostgreSQL as a Splink backend

Splink is compatible with using [PostgreSQL](https://www.postgresql.org/) (or simply as _Postgres_) as a SQL backend - for other options have a look at the [overview of Splink backends](./backends.md).

## Setup

Splink makes use of [SQLAlchemy](https://www.sqlalchemy.org/) for connecting to Postgres, and the default database adapter is [`psycopg2`](https://www.psycopg.org/docs/index.html), but you should be able to use any other if you prefer. The `PostgresLinker` requires a valid [engine](https://docs.sqlalchemy.org/en/20/core/connections.html) upon creation to manage interactions with the database:
```py
from sqlalchemy import create_engine

from splink.postgres.linker import PostgresLinker
import splink.postgres.comparison_library as cl

# create a sqlalchemy engine to manage connecting to the database
engine = create_engine("postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DB_NAME")

settings = {
    "link_type": "dedupe_only",
}
```

You can pass data to the linker in one of two ways:

* use the name of a pre-existing table in your database
```py
linker = PostgresLinker("my_data_table", settings, engine=engine)
```

* or pass a pandas DataFrame directly, in which case the linker will create a corresponding table for you automatically in the database
```py
import pandas as pd

# create pandas frame from csv
df = pd.read_csv("./my_data_table.csv")

linker = PostgresLinker(df, settings, engine=engine)
```

## Permissions

When you connect to Postgres, you must do so with a role that has sufficient privileges for Splink to operate correctly. These are:

* `CREATE ON DATABASE`, to allow Splink to create a schema for working, and install the `fuzzystrmatch` extension
* `USAGE ON LANGUAGE SQL` and `USAGE ON TYPE float8` - these are required for creating the [UDFs](#user-defined-functions-udfs) that Splink employs for calculations

## Things to know

### Schemas

When you create a `PostgresLinker`, Splink will create a new schema within the database you specify - by default this schema is called `splink`, but you can choose another name by passing the appropriate argument when creating the linker:
```py
linker = PostgresLinker(df, settings, engine=engine, schema="another_splink_schema")
```
This schema is where all of Splink's work will be carried out, and where any tables created by Splink will live.

By default when _looking_ for tables, Splink will check the schema it created, and the `public` schema; if you have tables in other schemas that you would like to be discoverable by Splink, you can use the parameter `other_schemas_to_search`:
```py
linker = PostgresLinker(df, settings, engine=engine, other_schemas_to_search=["my_data_schema_1", "my_data_schema_2"])
```

### User-Defined Functions (UDFs)

Splink makes use of Postgres' user-defined functions in order to operate, which are defined in the [schema created by Splink](#schemas) when you create the linker. These functions are all defined using SQL, and are:

* `log2` - required for core Splink functionality
* `datediff` - for [the datediff comparison level](../../../comparison_level_library.md#splink.comparison_level_library.DatediffLevelBase)
* `ave_months_between` - for [the datediff comparison level](../../../comparison_level_library.md#splink.comparison_level_library.DatediffLevelBase)
* `array_intersect` - for [the array intersect comparison level](../../../comparison_level_library.md#splink.comparison_level_library.ArrayIntersectLevelBase)

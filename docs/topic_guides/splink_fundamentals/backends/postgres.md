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

settings = SettingsCreator(
    link_type= "dedupe_only",
)
```

You can pass data to the linker in one of two ways:

* use the name of a pre-existing table in your database
```py
dbapi = PostgresAPI(engine=engine)
linker = Linker(
    "my_data_table,
    settings_dict,
    db_api=db_api,
)
```


* or pass a pandas DataFrame directly, in which case the linker will create a corresponding table for you automatically in the database
```py
import pandas as pd

# create pandas frame from csv
df = pd.read_csv("./my_data_table.csv")

dbapi = PostgresAPI(engine=engine)
linker = Linker(
    df,
    settings_dict,
    db_api=db_api,
)
```

## Permissions

When you connect to Postgres, you must do so with a role that has sufficient privileges for Splink to operate correctly. These are:

* `CREATE ON DATABASE`, to allow Splink to create a schema for working, and install the `fuzzystrmatch` extension
* `USAGE ON LANGUAGE SQL` and `USAGE ON TYPE float8` - these are required for creating the [UDFs](#user-defined-functions-udfs) that Splink employs for calculations

## Things to know

### Schemas

When you create a `PostgresLinker`, Splink will create a new schema within the database you specify - by default this schema is called `splink`, but you can choose another name by passing the appropriate argument when creating the linker:
```py
dbapi = PostgresAPI(engine=engine, schema="another_splink_schema")
```
This schema is where all of Splink's work will be carried out, and where any tables created by Splink will live.

By default when _looking_ for tables, Splink will check the schema it created, and the `public` schema; if you have tables in other schemas that you would like to be discoverable by Splink, you can use the parameter `other_schemas_to_search`:
```py
dbapi = PostgresAPI(engine=engine, other_schemas_to_search=["my_data_schema_1", "my_data_schema_2"])
```

### User-Defined Functions (UDFs)

Splink makes use of Postgres' user-defined functions in order to operate, which are defined in the [schema created by Splink](#schemas) when you create the linker. These functions are all defined using SQL, and are:

* `log2` - required for core Splink functionality
* `datediff` - for [the datediff comparison level](../../../api_docs/comparison_level_library.md)
* `ave_months_between` - for [the datediff comparison level](../../../api_docs/comparison_level_library.md)
* `array_intersect` - for [the array intersect comparison level](../../../api_docs/comparison_level_library.md)

!!! information Note
    The information below is only relevant if you are planning on [making changes to Splink](../../../dev_guides/index.md). If you are only intending to _use_ Splink with Postgres, you do not need to read any further.

## Testing Splink with Postgres

To run only the Splink tests that run against Postgres, you can run simply:
```bash
pytest -m postgres_only tests/
```
For more information see the [documentation page for testing in Splink](../../../dev_guides/changing_splink/testing.md#running-tests-for-specific-backends-or-backend-groups).

The tests will are run using a temporary database and user that are created at the start of the test session, and destroyed at the end.

### Postgres via docker

If you are trying to [run tests with Splink](../../../dev_guides/changing_splink/testing.md) on Postgres, or simply develop using Postgres, you may prefer to not actually [install Postgres on you system](https://www.postgresql.org/download/), but to run it instead using [Docker](https://www.docker.com/).
In this case you can simply run the setup script (a thin wrapper around `docker-compose`):
```bash
./scripts/postgres_docker/setup.sh
```
Included in the docker-compose file is a [pgAdmin](https://www.pgadmin.org/) container to allow easy exploration of the database as you work, which can be accessed in-browser on the default port.

When you are finished you can remove these resources:
```bash
./scripts/postgres_docker/teardown.sh
```

### Running with a pre-existing database

If you have a pre-existing Postgres server you wish to use to run the tests against, you will need to specify environment variables for the credentials where they differ from default (in parentheses):

* `SPLINKTEST_PG_USER` (`splinkognito`)
* `SPLINKTEST_PG_PASSWORD` (`splink123!`)
* `SPLINKTEST_PG_HOST` (`localhost`)
* `SPLINKTEST_PG_PORT` (`5432`)
* `SPLINKTEST_PG_DB` (`splink_db`) - tests will not actually run against this, but it is from a connection to this that the temporary test database + user will be created

While care has been taken to ensure that tests are run using minimal permissions, and are cleaned up after, it is probably wise to run tests connected to a non-important database, in case anything goes wrong.
In addition to the [above privileges](#permissions), in order to run the tests you will need:

* `CREATE DATABASE` to create a temporary testing database
* `CREATEROLE` to create a temporary user role with limited privileges, which will be actually used for all the SQL execution in the tests

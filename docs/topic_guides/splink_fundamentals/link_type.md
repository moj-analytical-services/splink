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

This guide shows how to specify settings and initialise the linker for the three link types. The examples assume you already have input data in `df`, or in `df_1`, `df_2` and `df_n`. Register each input with the same database API before passing it to `Linker`.

## Deduplication

The `dedupe_only` link type expects the user to provide a single input table, and is specified as follows

``` python
from splink import DuckDBAPI, Linker, SettingsCreator

settings = SettingsCreator(
    link_type= "dedupe_only",
)

db_api = DuckDBAPI()
df_splink = db_api.register(df, dataset_display_name="people")
linker = Linker(df_splink, settings)
```

## Link only

The `link_only` link type expects the user to provide a list of input tables, and is specified as follows:

``` python
from splink import DuckDBAPI, Linker, SettingsCreator

settings = SettingsCreator(
    link_type= "link_only",
)

db_api = DuckDBAPI()
input_tables = [
    db_api.register(df_1, dataset_display_name="dataset_1"),
    db_api.register(df_2, dataset_display_name="dataset_2"),
    db_api.register(df_n, dataset_display_name="dataset_n"),
]
linker = Linker(input_tables, settings)
```

Dataset labels used in the outputs (the `source_dataset` column) are set via the `dataset_display_name` argument when registering each table with the database API (e.g. `db_api.register(df_1, dataset_display_name="name1")`). If not provided at registration, defaults will be automatically chosen by Splink.

## Link and dedupe

The `link_and_dedupe` link type expects the user to provide a list of input tables, and is specified as follows:

``` python
from splink import DuckDBAPI, Linker, SettingsCreator

settings = SettingsCreator(
    link_type= "link_and_dedupe",
)

db_api = DuckDBAPI()
input_tables = [
    db_api.register(df_1, dataset_display_name="dataset_1"),
    db_api.register(df_2, dataset_display_name="dataset_2"),
    db_api.register(df_n, dataset_display_name="dataset_n"),
]
linker = Linker(input_tables, settings)
```

Dataset labels used in the outputs (the `source_dataset` column) are set via the `dataset_display_name` argument when registering each table with the database API (e.g. `db_api.register(df_1, dataset_display_name="name1")`). If not provided at registration, defaults will be automatically chosen by Splink.

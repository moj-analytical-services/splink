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

This guide shows how to specify the settings dictionary and initialise the linker for the three link types.

## Deduplication

The `dedupe_only` link type expects the user to provide a single input table, and is specified as follows

``` python
from splink import SettingsCreator

settings = SettingsCreator(
    link_type= "dedupe_only",
)

linker = Linker(df, settings, db_api=dbapi, )
```

## Link only

The `link_only` link type expects the user to provide a list of input tables, and is specified as follows:

``` python
from splink import SettingsCreator

settings = SettingsCreator(
    link_type= "link_only",
)

linker = Linker(
    [df_1, df_2, df_n],
    settings,
    db_api=dbapi,
    input_table_aliases=["name1", "name2", "name3"],
)
```

The `input_table_aliases` argument is optional and are used to label the tables in the outputs. If not provided, defaults will be automatically chosen by Splink.

## Link and dedupe

The `link_and_dedupe` link type expects the user to provide a list of input tables, and is specified as follows:

``` python
from splink import SettingsCreator

settings = SettingsCreator(
    link_type= "link_and_dedupe",
)

linker = Linker(
    [df_1, df_2, df_n],
    settings,
    db_api=dbapi,
    input_table_aliases=["name1", "name2", "name3"],
)
```

The `input_table_aliases` argument is optional and are used to label the tables in the outputs. If not provided, defaults will be automatically chosen by Splink.

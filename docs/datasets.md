---
tags:
  - API
  - Datasets
  - Examples
---

# In-built datasets

Splink has some datasets available for use to help you get up and running, test ideas, or explore Splink features.
To use, simply import `splink_data_sets`:
```py
from splink.datasets import splink_data_sets

df = splink_data_sets.fake_1000
```
which you can then use to set up a linker:
```py
from splink.datasets import splink_data_sets
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl

df = splink_data_sets.fake_1000
linker = DuckDBLinker(
    df,
    {
        "link_type": "dedupe_only",
        "comparisons": [cl.exact_match("first_name"), cl.exact_match("surname")],
    },
)
```

## `splink_data_sets`

Each attribute of `splink_data_sets` is a dataset available for use, which exists as a pandas `DataFrame`.
These datasets are not packaged directly with Splink, but instead are downloaded only when they are requested.
Once requested they are cached for future use.
The cache can be cleared using [`splink_data_utils`](#splink_data_utils-object), 
which also contains information on available datasets, and which have already been cached.

### Available datasets

The datasets available are listed below:

{% include-markdown "./includes/generated_files/datasets.md" %}

## `splink_data_utils` API

In addition to `splink_data_sets`, you can also import `splink_data_utils`, 
which has a few functions to help managing `splink_data_sets`.
This can be useful if you have limited internet connection and want to see what is already cached,
or if you need to clear cache items (e.g. if datasets were to be updated, or if space is an issue).

For example:
```py
from splink.datasets import splink_data_utils

splink_data_utils.show_downloaded_data()
splink_data_utils.clear_cache(['fake_1000'])
```

::: splink.datasets._SplinkDataUtils
    handler: python
    options:
      members:
        - list_downloaded_datasets
        - list_all_datasets
        - show_downloaded_data
        - clear_downloaded_data
      show_root_heading: false
      show_source: false
      heading_level: 3

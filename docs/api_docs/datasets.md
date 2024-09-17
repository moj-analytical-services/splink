---
tags:
  - API
  - Datasets
  - Examples
---

# In-built datasets

Splink has some datasets available for use to help you get up and running, test ideas, or explore Splink features.
To use, simply import `splink_datasets`:
```py
from splink import splink_datasets

df = splink_datasets.fake_1000
```
which you can then use to set up a linker:
```py
from splink splink_datasets, Linker, DuckDBAPI, SettingsCreator

df = splink_datasets.fake_1000
linker = Linker(
    df,
    SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.exact_match("first_name"),
            cl.exact_match("surname"),
        ],
    ),
    db_api=DuckDBAPI()
)
```

??? tip "Troubleshooting"

    If you get a `SSLCertVerificationError` when trying to use the inbuilt datasets, this can be fixed with the `ssl` package by running:

    `ssl._create_default_https_context = ssl._create_unverified_context`.

## `splink_datasets`

Each attribute of `splink_datasets` is a dataset available for use, which exists as a pandas `DataFrame`.
These datasets are not packaged directly with Splink, but instead are downloaded only when they are requested.
Once requested they are cached for future use.
The cache can be cleared using `splink_dataset_utils` (see below),
which also contains information on available datasets, and which have already been cached.

### Available datasets

The datasets available are listed below:

{% include-markdown "./includes/generated_files/datasets_table.md" %}


## `splink_dataset_labels`

Some of the `splink_datasets` have corresponding clerical labels to help assess model performance. These are requested through the `splink_dataset_labels` module.

### Available datasets

The datasets available are listed below:

{% include-markdown "./includes/generated_files/dataset_labels_table.md" %}


## `splink_dataset_utils` API

In addition to `splink_datasets`, you can also import `splink_dataset_utils`,
which has a few functions to help managing `splink_datasets`.
This can be useful if you have limited internet connection and want to see what is already cached,
or if you need to clear cache items (e.g. if datasets were to be updated, or if space is an issue).

For example:
```py
from splink.datasets import splink_dataset_utils

splink_dataset_utils.show_downloaded_data()
splink_dataset_utils.clear_cache(['fake_1000'])
```

::: splink.internals.datasets.utils.SplinkDataUtils
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

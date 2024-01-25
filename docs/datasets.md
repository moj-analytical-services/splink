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
from splink.datasets import splink_datasets

df = splink_datasets.fake_1000
```
which you can then use to set up a linker:
```py
from splink.datasets import splink_datasets
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl

df = splink_datasets.fake_1000
linker = DuckDBLinker(
    df,
    {
        "link_type": "dedupe_only",
        "comparisons": [cl.exact_match("first_name"), cl.exact_match("surname")],
    },
)
```

??? tip "Troubleshooting"

    If you get a `SSLCertVerificationError` when trying to use the inbuilt datasets, this can be fixed with the `ssl` package by running:
    
    `ssl._create_default_https_context = ssl._create_unverified_context`.

## `splink_datasets`

Each attribute of `splink_datasets` is a dataset available for use, which exists as a pandas `DataFrame`.
These datasets are not packaged directly with Splink, but instead are downloaded only when they are requested.
Once requested they are cached for future use.
The cache can be cleared using [`splink_dataset_utils`](#splink_dataset_utils-object), 
which also contains information on available datasets, and which have already been cached.

### Available datasets

The datasets available are listed below:

|dataset name|description|rows|unique entities|link to source|
|-|-|-|-|-|
|`fake_1000`|Fake 1000 from splink demos.  Records are 250 simulated people, with different numbers of duplicates, labelled.|1,000|250|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/fake_1000.csv)|
|`historical_50k`|The data is based on historical persons scraped from wikidata. Duplicate records are introduced with a variety of errors.|50,000|5,156|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/historical_figures_with_errors_50k.parquet)|
|`febrl3`|The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist of comparison patterns from an epidemiological cancer study in Germany.FEBRL3 data set contains 5000 records (2000 originals and 3000 duplicates), with a maximum of 5 duplicates based on one original record.|5,000|2,000|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/febrl/dataset3.csv)|
|`febrl4a`|The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist of comparison patterns from an epidemiological cancer study in Germany.FEBRL4a contains 5000 original records.|5,000|5,000|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/febrl/dataset4a.csv)|
|`febrl4b`|The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist of comparison patterns from an epidemiological cancer study in Germany.FEBRL4b contains 5000 duplicate records, one for each record in FEBRL4a.|5,000|5,000|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/febrl/dataset4b.csv)|
|`transactions_origin`|This data has been generated to resemble bank transactions leaving an account. There are no duplicates within the dataset and each transaction is designed to have a counterpart arriving in 'transactions_destination'. Memo is sometimes truncated or missing.|45,326|45,326|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/transactions_origin.parquet)|
|`transactions_destination`|This data has been generated to resemble bank transactions arriving in an account. There are no duplicates within the dataset and each transaction is designed to have a counterpart sent from 'transactions_origin'. There may be a delay between the source and destination account, and the amount may vary due to hidden fees and foreign exchange rates. Memo is sometimes truncated or missing.|45,326|45,326|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/transactions_destination.parquet)|



## `splink_dataset_labels`

Some of the `splink_datasets` have corresponding clerical labels to help assess model performance. These are requested through the `splink_dataset_labels` module.

### Available datasets

The datasets available are listed below:

|dataset name|description|rows|unique entities|link to source|
|-|-|-|-|-|
|`fake_1000_labels`|Clerical labels for fake_1000 |3,176|NA|[source](https://raw.githubusercontent.com/moj-analytical-services/splink_datasets/master/data/fake_1000_labels.csv)|



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

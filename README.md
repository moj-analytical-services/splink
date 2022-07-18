![image](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)
[![pypi](https://img.shields.io/github/v/release/moj-analytical-services/splink?include_prereleases)](https://pypi.org/project/splink/#history)
[![Downloads](https://pepy.tech/badge/splink/month)](https://pepy.tech/project/splink)
[![Documentation](https://img.shields.io/badge/API-documentation-blue)](https://moj-analytical-services.github.io/splink/)

# Fast, accurate and scalable probabilistic data linkage using your choice of SQL backend.

`splink` is a Python package for probabilistic record linkage (entity resolution).

Its key features are:

- It is extremely fast. It is capable of linking a million records on a laptop in around a minute.

- It is highly accurate, with support for term frequency adjustments, and sophisticated fuzzy matching logic.

- Linking jobs can be executed in Python (using the `DuckDB` package), or using big-data backends like `AWS Athena` and `Spark` to link 100+ million records.

- Training data is not required because models can be trained using an unsupervised approach.

- It produces a wide variety of interactive outputs, helping users to understand their model and diagnose linkage problems.

The core linkage algorithm is an implementation of Fellegi-Sunter's model of record linkage, with various customisations to improve accuracy.

## What does Splink do?

Splink deduplicates and links records from datasets that lack a unique identifier.

For example, a few of your records may look like this:

| row_id | first_name | surname | dob        | city       |
| ------ | ---------- | ------- | ---------- | ---------- |
| 1      | lucas      | smith   | 1984-01-02 | London     |
| 2      | lucas      | smyth   | 1984-07-02 | Manchester |
| 3      | lucas      | smyth   | 1984-07-02 |            |
| 4      | david      | jones   |            | Leeds      |
| 5      | david      | jones   | 1990-03-21 | Leeds      |

Splink produces pairwise predictions of the links:

| row_id_l | row_id_r | match_probability |
| -------- | -------- | ----------------- |
| 1        | 2        | 0.9               |
| 1        | 3        | 0.85              |
| 2        | 3        | 0.92              |
| 4        | 5        | 0.7               |

And clusters the predictions to produce an estimated unique id:

| cluster_id | row_id |
| ---------- | ------ |
| a          | 1      |
| a          | 2      |
| a          | 3      |
| b          | 4      |
| b          | 5      |

## Documentation

The homepage for the Splink documentation can be found [here](https://moj-analytical-services.github.io/splink/). Interactive demos can be found [here](https://github.com/moj-analytical-services/splink_demos/tree/splink3_demos), or by clicking the following Binder link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/splink3_demos?urlpath=lab)

The specification of the Fellegi Sunter statistical model behind `splink` is similar as that used in the R [fastLink package](https://github.com/kosukeimai/fastLink). Accompanying the fastLink package is an [academic paper](http://imai.fas.harvard.edu/research/files/linkage.pdf) that describes this model. A [series of interactive articles](https://www.robinlinacre.com/probabilistic_linkage/) also explores the theory behind Splink.

## Installation

Splink supports python 3.7+. To obtain the latest released version of splink:

```
pip install splink
```

## Quickstart

The following code demonstrates how to estimate the parameters of a deduplication model, and then use it to identify duplicate records.

For more detailed tutorials, please see [here](https://github.com/moj-analytical-services/splink_demos/tree/splink3_demos).

```
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    levenshtein_at_thresholds,
)

import pandas as pd
df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        levenshtein_at_thresholds("first_name", 2),
        exact_match("surname"),
        exact_match("dob"),
        exact_match("city", term_frequency_adjustments=True),
        exact_match("email"),
    ],
}

linker = DuckDBLinker(df, settings)
linker.estimate_u_using_random_sampling(target_rows=1e6)

blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

blocking_rule_for_training = "l.dob = r.dob"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

scored_comparisons = linker.predict()
scored_comparisons.as_pandas_dataframe(limit=5)

```

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing the initial funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are extremely grateful to professors Katie Harron, James Doidge and Peter Christen for their expert advice and guidance in the development of Splink. We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work. Any errors remain our own.

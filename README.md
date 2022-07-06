# Fast, accurate and scalable probabilistic data linkage using your choice of SQL backend.

![image](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)

`splink` is a Python package for probabilistic record linkage (entity resolution).

Its key features are:

- It is extremely fast. It is capable of linking a million records on a laptop in around a minute.

- It is highly accurate, with support for term frequency adjustments, and sophisticated fuzzy matching logic.

- It supports running linkage against multiple SQL backends, meaning it's capable of running at any scale. For smaller linkages of up to a few million records, no additional infrastructure is needed . For larger linkages, Splink currently supports Apache Spark or AWS Athena as backends.

- It produces a wide variety of interactive outputs, helping users to understand their model and diagnose linkage problems.

The core linkage algorithm is an implementation of Fellegi-Sunter's canonical model of record linkage, with various customisations to improve accuracy. Splink includes an implementation of the Expectation Maximisation algorithm, meaning that record linkage can be performed using an unsupervised approch (i.e. labelled training data is not needed).

## What does Splink do?

Suppose you have one or more datasets which contain records that refer to the same entity (e.g. a person). But your entities do not have a unique identifier, so you can't link them.

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

```

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing the initial funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work.

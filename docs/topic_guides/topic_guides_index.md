# User Guide

This section contains in-depth guides on a variety of topics and concepts within Splink, as well as data linking more generally. These are intended to provide an extra layer of detail on top of the Splink [tutorial](../demos/tutorials/00_Tutorial_Introduction.ipynb) and [examples](../demos/examples/examples_index.md).

The user guide is broken up into the following categories:

1. [Record Linkage Theory](./theory/record_linkage.md) - for an introduction to data linkage from a theoretical perspective, and to help build some intuition around the parameters being estimated in Splink models.
2. [Linkage Models in Splink](splink_fundamentals/backends/backends.md) - for an introduction to the building blocks of a Splink model. Including the supported SQL Backends and how to define a model with a Splink Settings dictionary.
3. [Data Preparation](data_preparation/feature_engineering.md) - for guidance on preparing your data for linkage. Including guidance on feature engineering to help improve Splink models.
4. [Blocking](blocking/blocking_rules.md) - for an introduction to Blocking Rules and their purpose within record linkage. Including how blocking rules are used in different contexts within Splink.
5. [Comparing Records](comparisons/customising_comparisons.ipynb) - for guidance on defining `Comparison`s withing a Splink model. Including how comparing records are structured within `Comparison`s, how to utilise string comparators for fuzzy matching and how deal with skewed data with Term Frequency Adjustments.
6. Model Training - for guidance on the methods for training a Splink model, and how to choose them for specific use cases. (Coming soon)
7. Clustering - for guidance on how records are clustered together. (Coming Soon)
8. [Evaluation](./evaluation/overview.md) - for guidance on how to evaluate Splink models, links and clusters (including Clerical Labelling).
9. [Performance](performance/drivers_of_performance.md) - for guidance on how to make Splink models run more efficiently.

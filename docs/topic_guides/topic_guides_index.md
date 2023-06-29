# Topic Guides

This section contains in-depth guides on a variety of topics and concepts within Splink, as well as data linking more generally. These are intended to provide an extra layer of detail ontop of the Splink [tutorial](../demos/00_Tutorial_Introduction.ipynb) and [examples](../examples_index.md).

The topic guides are broken up into the following categories:

1. [Record Linkage Theory](record_linkage.md) - for an introduction to data linkage from a theoretical perspective, and to help build some intuition around the parameters being estimated in Splink models.  
2. [Linkage Models in Splink](backends.md) - for an introduction to the building blocks of a Splink model. Including the supported SQL Backends and how to define a model with a Splink Settings dictionary.
3. [Data Preparation](./feature_engineering.md) - for guidance on perparing your data for linkage. Including guidance on feature engineering to help improve Splink models. 
4. [Comparing Records](./customising_comparisons.ipynb) - for guidance on defining `Comparison`s withing a Splink model. Including how comparing records are structured within `Comparison`s, how to utilise string comparators for fuzzy matching and how deal with skewed data with Term Frequency Adjustments.
5. [Blocking](./blocking_rules.md) - for an introduction to Blocking Rules and their purpose within record linkage. Including how blocking rules are used in different contexts within Splink.
6. [Performance](./optimising_spark.md) - for guidance on how to make Splink models run more efficiently.
7. Model Training - for guidance on the methods for training a Splink model, and how to choose them for specific use cases. (Coming soon)
8. Clustering - for guidance on how records are clustered together. (Coming Soon)
9. Model QA - for guidance on how to Quality Assure a Splink model & the resulting clusters. Including Clerical Labelling. (Coming Soon)
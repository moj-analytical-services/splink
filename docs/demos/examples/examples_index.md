---
hide:
  - toc
tags:
  - Examples
  - DuckDB
  - Spark
  - Athena
---

# Example Notebooks

This section provides a series of examples to help you get started with Splink. You can find the underlying notebooks in the [demos folder](https://github.com/moj-analytical-services/splink/tree/master/docs/demos/examples) of the Splink repo.

You can try these demos live in your web browser using the following link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab)

### :simple-duckdb: DuckDB examples

##### Entity type: Persons

[Deduplicating 50,000 records of realistic data based on historical persons](./duckdb/deduplicate_50k_synthetic.ipynb)

[Using the `link_only` setting to link, but not dedupe, two datasets](./duckdb/link_only.ipynb)

[Real time record linkage](./duckdb/real_time_record_linkage.ipynb)

[Accuracy analysis and ROC charts using a ground truth (cluster) column](./duckdb/accuracy_analysis_from_labels_column.ipynb)

[Estimating m probabilities from pairwise labels](./duckdb/pairwise_labels.ipynb)

[Deduplicating 50,000 records with Deterministic Rules](./duckdb/examples/duckdb/deterministic_dedupe.ipynb)

[Deduplicating the febrl3 dataset](./duckdb/febrl3.ipynb). Note this dataset comes from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), as referenced in A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

[Linking the febrl4 datasets](./duckdb/febrl4.ipynb). As above, these datasets are from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

##### Entity type: Financial transactions

[Linking financial transactions](./duckdb/transactions.ipynb)


### :simple-apachespark: PySpark examples

[Deduplication of a small dataset using PySpark. Entity type is persons.](./spark/deduplicate_1k_synthetic.ipynb)

### :simple-amazonaws: Athena examples

[Deduplicating 50,000 records of realistic data based on historical persons](./athena/deduplicate_50k_synthetic.ipynb)

### :simple-sqlite: SQLite examples

[Deduplicating 50,000 records of realistic data based on historical persons](./sqlite/deduplicate_50k_synthetic.ipynb)
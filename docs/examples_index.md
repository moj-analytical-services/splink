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

This section provides a series of examples to help you get started with splink. You can find the underlying notebooks at the [splink_demos](https://github.com/moj-analytical-services/splink_demos) repo.

You can try these demos live in your web browser using the following link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab)

### :simple-duckdb: DuckDB examples

##### Entity type: Persons

[Deduplicating 50,000 records of realistic data based on historical persons](./demos/example_deduplicate_50k_synthetic.ipynb)

[Using the `link_only` setting to link, but not dedupe, two datasets](./demos/example_link_only.ipynb)

[Real time record linkage](./demos/example_real_time_record_linkage.ipynb)

[Accuracy analysis and ROC charts using a ground truth (cluster) column](./demos/example_accuracy_analysis_from_labels_column.ipynb)

[Estimating m probabilities from pairwise labels](./demos/example_pairwise_labels.ipynb)

[Deduplicating the febrl3 dataset](./demos/example_febrl3.ipynb). Note this dataset comes from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), as referenced in A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

[Linking the febrl4 datasets](./demos/example_febrl4.ipynb). As above, these datasets are from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

##### Entity type: Financial transactions

[Linking financial transactions](./demos/example_transactions.ipynb)


### :simple-apachespark: PySpark examples

[Deduplication of a small dataset using Pyspark. Entity type is persons.](./demos/example_simple_pyspark.ipynb)

### :simple-amazonaws: Athena examples

[Deduplicating 50,000 records of realistic data based on historical persons](./demos/athena_deduplicate_50k_synthetic.ipynb)
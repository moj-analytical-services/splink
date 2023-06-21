---
tags:
  - Examples
  - DuckDB
  - Spark
---

### Examples

This page provides a series of examples to help you get started with splink. You can find the underlying notebooks at the [splink_demos](https://github.com/moj-analytical-services/splink_demos) repo.

You can try these demos live in your web browser using the following link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab)

### :simple-duckdb: DuckDB examples

##### Entity type: Persons

[Deduplicating 50,000 records of realistic data based on historical persons](https://moj-analytical-services.github.io/splink/demos/example_deduplicate_50k_synthetic.html)

[Using the `link_only` setting to link, but not dedupe, two datasets](https://moj-analytical-services.github.io/splink/demos/example_link_only.html)

[Real time record linkage](https://moj-analytical-services.github.io/splink/demos/example_real_time_record_linkage.html)

[Accuracy analysis and ROC charts using a ground truth (cluster) column](https://moj-analytical-services.github.io/splink/demos/example_accuracy_analysis_from_labels_column.html)

[Estimating m probabilities from pairwise labels](https://moj-analytical-services.github.io/splink/demos/example_pairwise_labels.html)

[Deduplicating the febrl3 dataset](https://moj-analytical-services.github.io/splink/demos/example_febrl3.html). Note this dataset comes from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), as referenced in A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

[Linking the febrl4 datasets](https://moj-analytical-services.github.io/splink/demos/example_febrl4.html). As above, these datasets are from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).

##### Entity type: Financial transactions

[Linking financial transactions](https://moj-analytical-services.github.io/splink/demos/example_transactions.html)

### :simple-apachespark: PySpark examples

[Deduplication of a small dataset using Pyspark. Entity type is persons.](https://moj-analytical-services.github.io/splink/demos/example_simple_pyspark.html)

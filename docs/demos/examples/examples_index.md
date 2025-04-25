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

This section provides a series of examples to help you get started with Splink. You can find the underlying notebooks in the [demos folder](https://github.com/moj-analytical-services/splink/tree/master/docs/demos/examples) of the Splink repository.

### :simple-duckdb: DuckDB examples

##### Entity type: Persons

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/deduplicate_50k_synthetic.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Deduplicating 50,000 records of realistic data based on historical persons](./duckdb/deduplicate_50k_synthetic.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/link_only.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Using the `link_only` setting to link, but not dedupe, two datasets](./duckdb/link_only.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/real_time_record_linkage.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Real time record linkage](./duckdb/real_time_record_linkage.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/accuracy_analysis_from_labels_column.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Accuracy analysis and ROC charts using a ground truth (cluster) column](./duckdb/accuracy_analysis_from_labels_column.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/pairwise_labels.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Estimating m probabilities from pairwise labels](./duckdb/pairwise_labels.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/deterministic_dedupe.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Deduplicating 50,000 records with Deterministic Rules](./duckdb/deterministic_dedupe.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/febrl3.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/> [Deduplicating the febrl3 dataset](./duckdb/febrl3.ipynb). Note this dataset comes from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), as referenced in A.2 [here](https://arxiv.org/pdf/2008.04443.pdf) and replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).
</a>

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/febrl4.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/> [Linking the febrl4 datasets](./duckdb/febrl4.ipynb). As above, these datasets are from [febrl](http://users.cecs.anu.edu.au/~Peter.Christen/Febrl/febrl-0.3/febrldoc-0.3/manual.html), replicated [here](https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html).
</a>

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb_no_test/cookbook.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Cookbook of various Splink techniques](./duckdb_no_test/cookbook.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb_no_test/comparison_playground.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Interactive comparison playground](./duckdb_no_test/comparison_playground.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb_no_test/cookbook.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Investigating Bias in a Splink Model](./duckdb_no_test/bias_eval.ipynb)

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb_no_test/pseudopeople-acs.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/> [Linking the pseudopeople Census and ACS datasets](./duckdb_no_test/pseudopeople-acs.ipynb). These datasets are generated using [pseudopeople](https://pseudopeople.readthedocs.io/en/latest/).
</a>


##### Entity type: Financial transactions

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb/transactions.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Linking financial transactions](./duckdb/transactions.ipynb)

##### Entity type: Businesses

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/duckdb_no_test/business_rates_match.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Matching business rates data with Companies House data](./duckdb_no_test/business_rates_match.ipynb)


### :simple-apachespark: PySpark examples

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/spark/deduplicate_1k_synthetic.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Deduplication of a small dataset using PySpark. Entity type is persons.](./spark/deduplicate_1k_synthetic.ipynb)

### :simple-amazonaws: Athena examples


</a> [Deduplicating 50,000 records of realistic data based on historical persons](./athena/deduplicate_50k_synthetic.ipynb)

### :simple-sqlite: SQLite examples

<a target="_blank" href="https://colab.research.google.com/github/moj-analytical-services/splink/blob/master/docs/demos/examples/sqlite/deduplicate_50k_synthetic.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a> [Deduplicating 50,000 records of realistic data based on historical persons](./sqlite/deduplicate_50k_synthetic.ipynb)

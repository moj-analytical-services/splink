<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink Logo" height="150px">
</p>

[![pypi](https://img.shields.io/github/v/release/moj-analytical-services/splink?include_prereleases)](https://pypi.org/project/splink/#history)
[![Downloads](https://pepy.tech/badge/splink/month)](https://pepy.tech/project/splink)
[![Documentation](https://img.shields.io/badge/API-documentation-blue)](https://moj-analytical-services.github.io/splink/)

# Fast, accurate and scalable probabilistic data linkage

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets without unique identifiers.

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in approximately one minute.  
🎯 **Accuracy:** Full support for term frequency adjustments and user-defined fuzzy matching logic.  
🌐 **Scalability:** Execute linkage jobs in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.  
🎓 **Unsupervised Learning:** No training data is required, as models can be trained using an unsupervised approach.  
📊 **Interactive Outputs:** Provides a wide range of interactive outputs to help users understand their model and diagnose linkage problems.

Splink's core linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customizations to improve accuracy.

## What does Splink do?

Consider the following records that lack a unique person identifier:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/main_readme_what_does_splink_do_1.drawio.png)

Splink predicts which rows link together:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/main_readme_what_does_splink_do_2.drawio.png)

and clusters these links to produce an estimated person ID:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/main_readme_what_does_splink_do_3.drawio.png)

## What data does Splink work best with?

Before using Splink, input data should be standardized, with consistent column names and formatting (e.g., lowercased, punctuation cleaned up, etc.).

Splink performs best with input data containing **multiple** columns that are **not highly correlated**. For instance, if the entity type is persons, you may have columns for full name, date of birth, and city. If the entity type is companies, you could have columns for name, turnover, sector, and telephone number.

High correlation occurs when the value of a column is highly constrained (predictable) from the value of another column. For example, a 'city' field is almost perfectly correlated with 'postcode'. Gender is highly correlated with 'first name'. Correlation is particularly problematic if **all** of your input columns are highly correlated.

Splink is not designed for linking a single column containing a 'bag of words'. For example, a table with a single 'company name' column, and no other details.

## Documentation

The homepage for the Splink documentation can be found [here](https://moj-analytical-services.github.io/splink/). Interactive demos can be found [here](https://github.com/moj-analytical-services/splink_demos/tree/splink3_demos), or by clicking the following Binder link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab)

The specification of the Fellegi Sunter statistical model behind `splink` is similar as that used in the R [fastLink package](https://github.com/kosukeimai/fastLink). Accompanying the fastLink package is an [academic paper](http://imai.fas.harvard.edu/research/files/linkage.pdf) that describes this model. A [series of interactive articles](https://www.robinlinacre.com/probabilistic_linkage/) also explores the theory behind Splink.

The Office for National Statistics have written a [case study about using Splink](https://github.com/Data-Linkage/Splink-census-linkage/blob/main/SplinkCaseStudy.pdf) to link 2021 Census data to itself.

## Installation

Splink supports python 3.7+. To obtain the latest released version of splink you can install from PyPI using pip:

```sh
pip install splink
```

or, if you prefer, you can instead install splink using conda:

```sh
conda install -c conda-forge splink
```

## Quickstart

The following code demonstrates how to estimate the parameters of a deduplication model, use it to identify duplicate records, and then use clustering to generate an estimated unique person ID.

For more detailed tutorials, please see [here](https://moj-analytical-services.github.io/splink/demos/00_Tutorial_Introduction.html).

```py
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
linker.estimate_u_using_random_sampling(max_pairs=1e6)

blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

blocking_rule_for_training = "l.dob = r.dob"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

pairwise_predictions = linker.predict()

clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.95)
clusters.as_pandas_dataframe(limit=5)
```

## Videos

- [A introductory presentation on Splink](https://www.youtube.com/watch?v=msz3T741KQI)
- [An introduction to the Splink Comparison Viewer dashboard](https://www.youtube.com/watch?v=DNvCMqjipis)

## Support

Please post on the [discussion forums](https://github.com/moj-analytical-services/splink/discussions) if you have any questions. If you think you have found a bug, pleaase raise an [issue](https://github.com/moj-analytical-services/splink/issues).

## Awards

🥇 Analysis in Government Awards 2020: Innovative Methods: [Winner](https://www.gov.uk/government/news/launch-of-the-analysis-in-government-awards)

🥇 MoJ DASD Awards 2020: Innovation and Impact - Winner

🥇 Analysis in Government Awards 2022: People's Choice Award - [Winner](https://analysisfunction.civilservice.gov.uk/news/announcing-the-winner-of-the-first-analysis-in-government-peoples-choice-award/)

🥈 Analysis in Government Awards 2022: Innovative Methods [Runner up](https://twitter.com/gov_analysis/status/1616073633692274689?s=20&t=6TQyNLJRjnhsfJy28Zd6UQ)

## Citation

If you use Splink in your research, we'd be grateful for a citation as follows:

```BibTeX
@article{Linacre_Lindsay_Manassis_Slade_Hepworth_2022,
	title        = {Splink: Free software for probabilistic record linkage at scale.},
	author       = {Linacre, Robin and Lindsay, Sam and Manassis, Theodore and Slade, Zoe and Hepworth, Tom},
	year         = 2022,
	month        = {Aug.},
	journal      = {International Journal of Population Data Science},
	volume       = 7,
	number       = 3,
	doi          = {10.23889/ijpds.v7i3.1794},
	url          = {https://ijpds.org/article/view/1794},
}
```

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing the initial funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are extremely grateful to professors Katie Harron, James Doidge and Peter Christen for their expert advice and guidance in the development of Splink. We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work. Any errors remain our own.

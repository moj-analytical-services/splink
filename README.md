<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink Logo" height="150px">
</p>

[![pypi](https://img.shields.io/github/v/release/moj-analytical-services/splink?include_prereleases)](https://pypi.org/project/splink/#history)
[![Downloads](https://static.pepy.tech/badge/splink/month)](https://pepy.tech/project/splink)
[![Documentation](https://img.shields.io/badge/API-documentation-blue)](https://moj-analytical-services.github.io/splink/)



# Fast, accurate and scalable probabilistic data linkage

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets that lack unique identifiers.

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in around a minute.  
🎯 **Accuracy:** Support for term frequency adjustments and user-defined fuzzy matching logic.  
🌐 **Scalability:** Execute linkage in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.  
🎓 **Unsupervised Learning:** No training data is required for model training.  
📊 **Interactive Outputs:** A suite of interactive visualisations help users understand their model and diagnose problems.  

Splink's linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customizations to improve accuracy.

## What does Splink do?

Consider the following records that lack a unique person identifier:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_1.drawio.png)

Splink predicts which rows link together:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_2.drawio.png)

and clusters these links to produce an estimated person ID:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_3.drawio.png)

## What data does Splink work best with?

Before using Splink, input data should be standardized, with consistent column names and formatting (e.g., lowercased, punctuation cleaned up, etc.).

Splink performs best with input data containing **multiple** columns that are **not highly correlated**. For instance, if the entity type is persons, you may have columns for full name, date of birth, and city. If the entity type is companies, you could have columns for name, turnover, sector, and telephone number.

High correlation occurs when the value of a column is highly constrained (predictable) from the value of another column. For example, a 'city' field is almost perfectly correlated with 'postcode'. Gender is highly correlated with 'first name'. Correlation is particularly problematic if **all** of your input columns are highly correlated.

Splink is not designed for linking a single column containing a 'bag of words'. For example, a table with a single 'company name' column, and no other details.

## Documentation

The homepage for the Splink documentation can be found [here](https://moj-analytical-services.github.io/splink/). Interactive demos can be found [here](https://github.com/moj-analytical-services/splink/tree/master/docs/demos), or by clicking the following Binder link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink/binder_branch?labpath=docs%2Fdemos%2Ftutorials%2F00_Tutorial_Introduction.ipynb)

The specification of the Fellegi Sunter statistical model behind `splink` is similar as that used in the R [fastLink package](https://github.com/kosukeimai/fastLink). Accompanying the fastLink package is an [academic paper](http://imai.fas.harvard.edu/research/files/linkage.pdf) that describes this model. The [Splink documentation site](https://moj-analytical-services.github.io/splink/topic_guides/fellegi_sunter.html) and a [series of interactive articles](https://www.robinlinacre.com/probabilistic_linkage/) also explores the theory behind Splink.

The Office for National Statistics have written a [case study about using Splink](https://github.com/Data-Linkage/Splink-census-linkage/blob/main/SplinkCaseStudy.pdf) to link 2021 Census data to itself.

## Installation

Splink supports python 3.8+. To obtain the latest released version of splink you can install from PyPI using pip:

```sh
pip install splink
```

or, if you prefer, you can instead install splink using conda:

```sh
conda install -c conda-forge splink
```

<details>
<summary><h3>Additional installation methods</h3></summary>

<br>

### Backend Specific Installs
From Splink v3.9.7, packages required by specific splink backends can be optionally installed by adding the `[<backend>]` suffix to the end of your `pip install`.

**Note** that **SQLite** and **DuckDB** come packaged with Splink and do not need to be optionally installed.

Backends supported by optional installs:

**Spark**
```sh
pip install 'splink[spark]'
```

**Athena**
```sh
pip install 'splink[athena]'
```

**PostgreSQL**
```sh
pip install 'splink[postgres]'
```

<br>

### DuckDBLess Splink
Should you require a more bare-bones version of Splink **without DuckDB**, please see the following area of the docs:
> [DuckDBless Splink Installation](https://moj-analytical-services.github.io/splink/installations.html#duckdb-less-installation)

</details>

## Quickstart

The following code demonstrates how to estimate the parameters of a deduplication model, use it to identify duplicate records, and then use clustering to generate an estimated unique person ID.

For more detailed tutorial, please see [here](https://moj-analytical-services.github.io/splink/demos/00_Tutorial_Introduction.html).

```py
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from splink.duckdb.blocking_rule_library import block_on
from splink.datasets import splink_datasets

df = splink_datasets.fake_1000

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        block_on("first_name"),
        block_on("surname"),
    ],
    "comparisons": [
        ctl.name_comparison("first_name"),
        ctl.name_comparison("surname"),
        ctl.date_comparison("dob", cast_strings_to_date=True),
        cl.exact_match("city", term_frequency_adjustments=True),
        ctl.email_comparison("email", include_username_fuzzy_level=False),
    ],
}

linker = DuckDBLinker(df, settings)
linker.estimate_u_using_random_sampling(max_pairs=1e6)

blocking_rule_for_training = block_on(["first_name", "surname"])

linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training, estimate_without_term_frequencies=True)

blocking_rule_for_training = block_on("dob")
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training, estimate_without_term_frequencies=True)

pairwise_predictions = linker.predict()

clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.95)
clusters.as_pandas_dataframe(limit=5)
```

## Videos

- [A introductory presentation on Splink](https://www.youtube.com/watch?v=msz3T741KQI)
- [An introduction to the Splink Comparison Viewer dashboard](https://www.youtube.com/watch?v=DNvCMqjipis)

## Charts Gallery

You can see all of the interactive charts provided in Splink by checking out the [Charts Gallery](./charts/index.md).

## Support

To find the best place to ask a question, report a bug or get general advice, please refer to our [Contributing Guide](./CONTRIBUTING.md).

## Awards

🥇 Analysis in Government Awards 2020: Innovative Methods - [Winner](https://www.gov.uk/government/news/launch-of-the-analysis-in-government-awards)

🥇 MoJ DASD Awards 2020: Innovation and Impact - Winner

🥇 Analysis in Government Awards 2022: People's Choice Award - [Winner](https://analysisfunction.civilservice.gov.uk/news/announcing-the-winner-of-the-first-analysis-in-government-peoples-choice-award/)

🥈 Analysis in Government Awards 2022: Innovative Methods - [Runner up](https://twitter.com/gov_analysis/status/1616073633692274689?s=20&t=6TQyNLJRjnhsfJy28Zd6UQ)

🥈 Civil Service Awards 2023: Best Use of Data, Science, and Technology - [Runner up](https://www.civilserviceawards.com/best-use-of-data-science-and-technology-award-2/)


## Citation

If you use Splink in your research, we'd be grateful for a citation as follows:

```BibTeX
@article{Linacre_Lindsay_Manassis_Slade_Hepworth_2022,
	title        = {Splink: Free software for probabilistic record linkage at scale.},
	author       = {Linacre, Robin and Lindsay, Sam and Manassis, Theodore and Slade, Zoe and Hepworth, Tom and Kennedy, Ross and Bond, Andrew},
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

## Related Repositories

While Splink is a standalone package, there are a number of repositories in the Splink ecosystem:


- [splink_scalaudfs](https://github.com/moj-analytical-services/splink_scalaudfs) contains the code to generate [User Defined Functions](https://moj-analytical-services.github.io/splink/dev_guides/udfs.html#spark) in scala which are then callable in Spark.
- [splink_datasets](https://github.com/moj-analytical-services/splink_datasets) contains datasets that can be installed automatically as a part of Splink through the [In-build datasets](https://moj-analytical-services.github.io/splink/datasets.html) functionality.
- [splink_synthetic_data](https://github.com/moj-analytical-services/splink_synthetic_data) contains code to generate synthetic data.

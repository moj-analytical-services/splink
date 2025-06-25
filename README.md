<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink Logo" height="150px">
</p>

[![pypi](https://img.shields.io/github/v/release/moj-analytical-services/splink?include_prereleases)](https://pypi.org/project/splink/#history)
[![Downloads](https://static.pepy.tech/badge/splink/month)](https://pepy.tech/project/splink)
[![Documentation](https://img.shields.io/badge/API-documentation-blue)](https://moj-analytical-services.github.io/splink/)


> [!IMPORTANT]
> 🎉 Splink 4 has been released! Examples of new syntax are [here](https://moj-analytical-services.github.io/splink/demos/examples/examples_index.html) and a release announcement is [here](https://moj-analytical-services.github.io/splink/blog/2024/07/24/splink-400-released.html).


# Fast, accurate and scalable data linkage and deduplication

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets that lack unique identifiers.

It is used widely by within government, academia and the private sector - see [use cases](https://moj-analytical-services.github.io/splink/#use-cases).

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in around a minute.<br>
🎯 **Accuracy:** Support for term frequency adjustments and user-defined fuzzy matching logic.<br>
🌐 **Scalability:** Execute linkage in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.<br>
🎓 **Unsupervised Learning:** No training data is required for model training.<br>
📊 **Interactive Outputs:** A suite of interactive visualisations help users understand their model and diagnose problems.<br>

Splink's linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customisations to improve accuracy.

## What does Splink do?

Consider the following records that lack a unique person identifier:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_1.drawio.png)

Splink predicts which rows link together:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_2.drawio.png)

and clusters these links to produce an estimated person ID:

![tables showing what splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_3.drawio.png)

## What data does Splink work best with?

Splink performs best with input data containing **multiple** columns that are **not highly correlated**. For instance, if the entity type is persons, you may have columns for full name, date of birth, and city. If the entity type is companies, you could have columns for name, turnover, sector, and telephone number.

High correlation occurs when one column is highly predictable from another - for instance, city can be predicted from postcode.  Correlation is particularly problematic if **all** of your input columns are highly correlated.

Splink is not designed for linking a single column containing a 'bag of words'. For example, a table with a single 'company name' column, and no other details.

## Documentation

The homepage for the Splink documentation can be found [here](https://moj-analytical-services.github.io/splink/), including a [tutorial](https://moj-analytical-services.github.io/splink/demos/tutorials/00_Tutorial_Introduction.html) and [examples](https://moj-analytical-services.github.io/splink/demos/examples/examples_index.html) that can be run in the browser.


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

### Installing Splink for Specific Backends


For projects requiring specific backends, Splink offers optional installations for **Spark**, **Athena**, and **PostgreSQL**. These can be installed by appending the backend name in brackets to the pip install command:
```sh
pip install 'splink[{backend}]'
```

<details>
<summary><i>Click here for backend-specific installation commands</i></summary>

#### Spark
```sh
pip install 'splink[spark]'
```

#### Athena
```sh
pip install 'splink[athena]'
```

#### PostgreSQL
```sh
pip install 'splink[postgres]'
```
</details>

## Quickstart

The following code demonstrates how to estimate the parameters of a deduplication model, use it to identify duplicate records, and then use clustering to generate an estimated unique person ID.

For more detailed tutorial, please see [here](https://moj-analytical-services.github.io/splink/demos/tutorials/00_Tutorial_Introduction.html).

```py
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

db_api = DuckDBAPI()

df = splink_datasets.fake_1000

settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.JaroWinklerAtThresholds("first_name", [0.9, 0.7]),
        cl.JaroAtThresholds("surname", [0.9, 0.7]),
        cl.DateOfBirthComparison(
            "dob",
            input_is_string=True,
            datetime_metrics=["year", "month"],
            datetime_thresholds=[1, 1],
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname"),
    ]
)

linker = Linker(df, settings, db_api)

linker.training.estimate_probability_two_random_records_match(
    [block_on("first_name", "surname")],
    recall=0.7,
)

linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

linker.training.estimate_parameters_using_expectation_maximisation(
    block_on("first_name", "surname")
)

linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))

pairwise_predictions = linker.inference.predict(threshold_match_weight=-10)

clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
    pairwise_predictions, 0.95
)

df_clusters = clusters.as_pandas_dataframe(limit=5)
```

## Videos

- [Pydata Global 2024 talk](https://www.youtube.com/watch?v=eQtFkI8f02U)
- [A introductory presentation on Splink](https://www.youtube.com/watch?v=msz3T741KQI)
- [An introduction to the Splink Comparison Viewer dashboard](https://www.youtube.com/watch?v=DNvCMqjipis)


## Support

To find the best place to ask a question, report a bug or get general advice, please refer to our [Guide](./CONTRIBUTING.md).

## Awards

🥈 Civil Service Awards 2023: Best Use of Data, Science, and Technology - [Runner up](https://www.civilserviceawards.com/best-use-of-data-science-and-technology-award-2/)

🥇 Analysis in Government Awards 2022: People's Choice Award - [Winner](https://analysisfunction.civilservice.gov.uk/news/announcing-the-winner-of-the-first-analysis-in-government-peoples-choice-award/)

🥈 Analysis in Government Awards 2022: Innovative Methods - [Runner up](https://twitter.com/gov_analysis/status/1616073633692274689?s=20&t=6TQyNLJRjnhsfJy28Zd6UQ)

🥇 Analysis in Government Awards 2020: Innovative Methods - [Winner](https://www.gov.uk/government/news/launch-of-the-analysis-in-government-awards)

🥇 MoJ Data and Analytical Services Directorate (DASD) Awards 2020: Innovation and Impact - Winner


## Citation

If you use Splink in your research, please cite as follows:

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

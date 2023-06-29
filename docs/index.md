---
hide:
  - navigation
  - toc
---

<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink Logo" style="max-width: 500px;">
</p>

# Fast, accurate and scalable probabilistic data linkage

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets without unique identifiers.


[Get Started with Splink](./getting_started.md){ .md-button .md-button--primary } 

<hr>

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in approximately one minute.  
🎯 **Accuracy:** Full support for term frequency adjustments and user-defined fuzzy matching logic.  
🌐 **Scalability:** Execute linkage jobs in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.  
🎓 **Unsupervised Learning:** No training data is required, as models can be trained using an unsupervised approach.  
📊 **Interactive Outputs:** Provides a wide range of interactive outputs to help users understand their model and diagnose linkage problems.  

Splink's core linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customizations to improve accuracy.

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

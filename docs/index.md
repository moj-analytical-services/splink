---
hide:
  - navigation
  - toc
---

<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink: data linkage at scale. (Splink logo)." style="max-width: 500px;">
</p>

!!! info

    🎉 Splink 4 has been released! Examples of new syntax are [here](./demos/examples/examples_index.md) and a release announcement is [here](./blog/posts/2024-07-10-splink4_release.md).


# Fast, accurate and scalable probabilistic data linkage

Splink is a Python package for probabilistic record linkage (entity resolution) that allows you to deduplicate and link records from datasets without unique identifiers.

[Get Started with Splink](./getting_started.md){ .md-button .md-button--primary }

<hr>

## Key Features

⚡ **Speed:** Capable of linking a million records on a laptop in approximately one minute.<br>
🎯 **Accuracy:** Full support for term frequency adjustments and user-defined fuzzy matching logic.<br>
🌐 **Scalability:** Execute linkage jobs in Python (using DuckDB) or big-data backends like AWS Athena or Spark for 100+ million records.<br>
🎓 **Unsupervised Learning:** No training data is required, as models can be trained using an unsupervised approach.<br>
📊 **Interactive Outputs:** Provides a wide range of interactive outputs to help users understand their model and diagnose linkage problems.<br>

Splink's core linkage algorithm is based on Fellegi-Sunter's model of record linkage, with various customizations to improve accuracy.

## What does Splink do?

Consider the following records that lack a unique person identifier:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_1.drawio.png)

Splink predicts which rows link together:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_2.drawio.png)

and clusters these links to produce an estimated person ID:

![tables showing what Splink does](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/README/what_does_splink_do_3.drawio.png)

## What data does Splink work best with?

Before using Splink, input data should be standardised, with consistent column names and formatting (e.g., lowercased, punctuation cleaned up, etc.).

Splink performs best with input data containing **multiple** columns that are **not highly correlated**. For instance, if the entity type is persons, you may have columns for full name, date of birth, and city. If the entity type is companies, you could have columns for name, turnover, sector, and telephone number.

High correlation occurs when the value of a column is highly constrained (predictable) from the value of another column. For example, a 'city' field is almost perfectly correlated with 'postcode'. Gender is highly correlated with 'first name'. Correlation is particularly problematic if **all** of your input columns are highly correlated.

Splink is not designed for linking a single column containing a 'bag of words'. For example, a table with a single 'company name' column, and no other details.

## Support

If after reading the documentatation you still have questions, please feel free to post on our [discussion forum](https://github.com/moj-analytical-services/splink/discussions).

## Use Cases

Here is a list of some of our known users and their use cases:

=== "Public Sector (UK)"

	- [Ministry of Justice](https://www.gov.uk/government/organisations/ministry-of-justice) created [linked datasets (combining courts, prisons and probation data)](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) for use by researchers as part of the [Data First programme](https://www.gov.uk/guidance/ministry-of-justice-data-first)
	- [Office for National Statistics](https://www.ons.gov.uk/)'s [Business Index](https://unece.org/sites/default/files/2023-04/ML2023_S1_UK_Breton_A.pdf) (formerly the Inter Departmental Business Register), [Demographic Index](https://uksa.statisticsauthority.gov.uk/wp-content/uploads/2023/02/EAP182-Quality-work-for-Demographic-Index-MDQA.pdf) and the [2021 Census](https://github.com/Data-Linkage/Splink-census-linkage/blob/main/SplinkCaseStudy.pdf).  See also [this article](https://www.government-transformation.com/data/interview-modernizing-public-sector-insight-through-automated-linkage).
	- [Lewisham Council](https://lewisham.gov.uk/) (London) [identified and auto-enrolled over 500 additional eligible families](https://lewisham.gov.uk/articles/news/extra-funding-for-lewisham-schools-in-pilot-data-project) to receive Free School Meals
	- [London Office of Technology and Innovation](https://loti.london/) created a dashboard to help [better measure and reduce rough sleeping](https://loti.london/projects/rough-sleeping-insights-project/) across London
	- [Competition and Markets Authority](https://www.gov.uk/government/organisations/competition-and-markets-authority) identified ['Persons with Significant Control' and estimated ownership groups](https://assets.publishing.service.gov.uk/media/626ab6c4d3bf7f0e7f9d5a9b/220426_Annex_-State_of_Competition_Appendices_FINAL.pdf) across companies
	- [Office for Health Improvement and Disparities](https://www.gov.uk/government/organisations/office-for-health-improvement-and-disparities) linked Health and Justice data to [assess the pathways between probation and specialist alcohol and drug treatment services](https://www.gov.uk/government/statistics/pathways-between-probation-and-addiction-treatment-in-england#:~:text=Details,of%20Health%20and%20Social%20Care) as part of the [Better Outcomes through Linked Data programme](https://www.gov.uk/government/publications/ministry-of-justice-better-outcomes-through-linked-data-bold)
	- [Ministry of Defence](https://www.gov.uk/government/organisations/ministry-of-defence) recently launched their [Veteran's Card system](https://www.gov.uk/government/news/hm-armed-forces-veteran-cards-will-officially-launch-in-the-new-year-following-a-successful-assessment-from-the-central-digital-and-data-office) which uses Splink to verify applicants against historic records. This project was shortlisted for the [Civil Service Awards](https://www.civilserviceawards.com/creative-solutions-award/)
 	- [Gateshead Council](https://www.gateshead.gov.uk/), in partnership with the [National Innovation Centre for Data](https://www.nicd.org.uk/) are creating a [single view of debt](https://nicd.org.uk/knowledge-hub/an-end-to-end-guide-to-overcoming-unique-identifier-challenges-with-splink)

=== "Public Sector (International)"

	- The German Federal Statistical Office ([Destatis](https://www.destatis.de/EN/Home/_node.html)) uses Splink to conduct projects in linking register-based census data.
	- The [European Medicines Agency](https://www.ema.europa.eu/en/homepage) uses Splink to detect duplicate adverse event reports for veterinary medicines
	- [Chilean Ministry of Health](https://www.gob.cl/en/ministries/ministry-of-health/) and [University College London](https://www.ucl.ac.uk/) have [assessed the access to immunisation programs among the migrant population](https://ijpds.org/article/view/2348)
	- [Florida Cancer Registry](https://www.floridahealth.gov/diseases-and-conditions/cancer/cancer-registry/index.html), published a [feasibility study](https://scholar.googleusercontent.com/scholar?q=cache:sADwxy-D75IJ:scholar.google.com/+splink+florida&hl=en&as_sdt=0,5) which showed Splink was faster and more accurate than alternatives
	- [Catalyst Cooperative](https://catalyst.coop)'s [Public Utility Data Liberation Project](https://github.com/catalyst-cooperative/pudl) links public financial and operational data from electric utilities for use by US climate advocates, policymakers, and researchers seeking to accelerate the transition away from fossil fuels.

=== "Academia"

	- [Stanford University](https://www.stanford.edu/) investigated the impact of [receiving government assistance has on political attitudes](https://www.cambridge.org/core/journals/american-political-science-review/article/abs/does-receiving-government-assistance-shape-political-attitudes-evidence-from-agricultural-producers/39552BC5A496EAB6CB484FCA51C6AF21)
	- [Bern University](https://arbor.bfh.ch/) researched how [Active Learning can be applied to Biomedical Record Linkage](https://ebooks.iospress.nl/doi/10.3233/SHTI230545)

=== "Other"
	- [Marie Curie](https://podcasts.apple.com/gb/podcast/unlocking-data-at-marie-curie/id1724979056?i=1000649964922) have used Splink to build a single customer view on fundraising data which has been a "huge success [...] the tooling is just so much better. [...] The power of being able to select, plug in, configure and train a tool versus writing code. It's just mind boggling actually."  Amongst other benefits, the system is expected to "dramatically reduce manual reporting efforts previously required". See also the blog post [here](https://esynergy.co.uk/our-work/marie-curie/).
 	- [Club Brugge](https://www.clubbrugge.be/en) uses Splink to link football players from different data providers to their own database, simplifying and reducing the need for manual linkage labor.

Sadly, we don't hear about the majority of our users or what they are working on. If you have a use case and it is not shown here please [add it to the list](https://github.com/moj-analytical-services/splink/edit/master/docs/index.md)!

## Awards

🥈 Civil Service Awards 2023: Best Use of Data, Science, and Technology - [Runner up](https://www.civilserviceawards.com/best-use-of-data-science-and-technology-award-2/)

🥇 Analysis in Government Awards 2022: People's Choice Award - [Winner](https://analysisfunction.civilservice.gov.uk/news/announcing-the-winner-of-the-first-analysis-in-government-peoples-choice-award/)

🥈 Analysis in Government Awards 2022: Innovative Methods - [Runner up](https://twitter.com/gov_analysis/status/1616073633692274689?s=20&t=6TQyNLJRjnhsfJy28Zd6UQ)

🥇 Analysis in Government Awards 2020: Innovative Methods - [Winner](https://www.gov.uk/government/news/launch-of-the-analysis-in-government-awards)

🥇 Ministry of Justice Data and Analytical Services Directorate (DASD) Awards 2020: Innovation and Impact - Winner


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

---
hide:
  - navigation
  - toc
---

<p align="center">
<img src="https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png" alt="Splink: data linkage at scale. (Splink logo)." style="max-width: 500px;">
</p>

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

## Videos

Our PyData Global 2024 talk provides a brief introduction to Splink and is available on YouTube [here](https://www.youtube.com/watch?v=eQtFkI8f02U).

## Support

If after reading the documentatation you still have questions, please feel free to post on our [discussion forum](https://github.com/moj-analytical-services/splink/discussions).

## Use Cases

Here is a list of some of our known users and their use cases:

=== "Public Sector (UK)"

	- [Office for National Statistics](https://www.ons.gov.uk/)'s [Business Index](https://unece.org/sites/default/files/2023-04/ML2023_S1_UK_Breton_A.pdf) (formerly the Inter Departmental Business Register), [Demographic Index](https://uksa.statisticsauthority.gov.uk/wp-content/uploads/2023/02/EAP182-Quality-work-for-Demographic-Index-MDQA.pdf) and the [2021 Census](https://github.com/Data-Linkage/Splink-census-linkage/blob/main/SplinkCaseStudy.pdf).  See also [this article](https://www.government-transformation.com/data/interview-modernizing-public-sector-insight-through-automated-linkage) and [2021 Census to PDS linkage report](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthinequalities/methodologies/census2021topersonaldemographicsservicelinkagereport).
	- [NHS England](https://www.england.nhs.uk/) is working on developing an alternative data linkage model using splink as the core engine for a new probabilistic data linkage service. This is in order to improve linkage and linkage explainability across NHS datasets. Code now available on [github](https://github.com/nhsengland/NHSE_probabilistic_linkage).
	- [Ministry of Defence](https://www.gov.uk/government/organisations/ministry-of-defence) launched their [Veteran's Card system](https://www.gov.uk/government/news/hm-armed-forces-veteran-cards-will-officially-launch-in-the-new-year-following-a-successful-assessment-from-the-central-digital-and-data-office) which uses Splink to verify applicants against historic records. This project was shortlisted for the [Civil Service Awards](https://www.civilserviceawards.com/creative-solutions-award/)
	- [Ministry of Justice](https://www.gov.uk/government/organisations/ministry-of-justice) created [linked datasets (combining courts, prisons and probation data)](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) for use by researchers as part of the [Data First programme](https://www.gov.uk/guidance/ministry-of-justice-data-first)
	- [UK Health Security Agency](https://www.gov.uk/government/organisations/uk-health-security-agency) [used Splink](https://www.gov.uk/government/publications/bloodborne-viruses-opt-out-testing-in-emergency-departments/appendix-for-emergency-department-bloodborne-virus-opt-out-testing-12-month-interim-report-2023#:~:text=Appendix%202D%3A%20public%20health%20evaluation%20data%20linkage%20methodology) to link HIV testing data to national health records to [evaluate the impact of emergency department opt-out bloodborne virus testing](https://www.gov.uk/government/publications/bloodborne-viruses-opt-out-testing-in-emergency-departments/public-health-evaluation-of-bbv-opt-out-testing-in-eds-in-england-24-month-interim-report).
	- The Department for Education uses Splink to match records from certain data providers to existing learners and reduce the volume of clerical work required for corrections
	- [SAIL Databank](https://saildatabank.com/), in collaboration with [Secure eResearch Platform (SeRP)](https://serp.ac.uk/), uses Splink to produce linked cohorts for a wide range of population-level research applications
	- [Lewisham Council](https://lewisham.gov.uk/) (London) [identified and auto-enrolled over 500 additional eligible families](https://lewisham.gov.uk/articles/news/extra-funding-for-lewisham-schools-in-pilot-data-project) to receive Free School Meals
	- [Leicestershire County Council](https://www.leicestershire.gov.uk/) use Splink to match individuals across their Education and Social Care systems.  This ensures triage and front-line practitioners have a complete picture of those individuals.
	- [Integrated Corporate Services](https://icsdigital.blog.gov.uk/2024/05/24/introducing-ics-digital/) have used Splink to match address data in historical datasets, substantially improving match rates.
	- [London Office of Technology and Innovation](https://loti.london/) created a dashboard to help [better measure and reduce rough sleeping](https://loti.london/projects/rough-sleeping-insights-project/) across London
	- [Competition and Markets Authority](https://www.gov.uk/government/organisations/competition-and-markets-authority) identified ['Persons with Significant Control' and estimated ownership groups](https://assets.publishing.service.gov.uk/media/626ab6c4d3bf7f0e7f9d5a9b/220426_Annex_-State_of_Competition_Appendices_FINAL.pdf) across companies
	- [Office for Health Improvement and Disparities](https://www.gov.uk/government/organisations/office-for-health-improvement-and-disparities) linked Health and Justice data to [assess the pathways between probation and specialist alcohol and drug treatment services](https://www.gov.uk/government/statistics/pathways-between-probation-and-addiction-treatment-in-england#:~:text=Details,of%20Health%20and%20Social%20Care) as part of the [Better Outcomes through Linked Data programme](https://www.gov.uk/government/publications/ministry-of-justice-better-outcomes-through-linked-data-bold)
 	- [Gateshead Council](https://www.gateshead.gov.uk/), in partnership with the [National Innovation Centre for Data](https://www.nicd.org.uk/) are creating a [single view of debt](https://nicd.org.uk/knowledge-hub/an-end-to-end-guide-to-overcoming-unique-identifier-challenges-with-splink)
	- [Homes England](https://www.gov.uk/government/organisations/homes-england) has been working with the new developed Splink address matching version. We have succesfully tested and checked the linkage between Land Registry Price Paid dataset and the new Ordnance Survey National Geographical Dataset (NGD) but adddresses. The current linkage performs around 30 Million records in less than 5 hours with a high accuracy in a Databricks environment. This is helping Homes England with a vital component to identify and monitor new builds that will contribute to the 1.5 M homes mandate.
	- The [Department for Business and Trade](https://www.gov.uk/government/organisations/department-for-business-and-trade) plans to use Splink as part of [Matchbox](https://github.com/uktrade/matchbox) to reconcile business and product data for both analytical and operational use
 	- The [Welsh Revenue Authority](https://www.gov.wales/welsh-revenue-authority) uses Splink in multiple linkage workflows to identify links in their own data, as well as to third party data for operational support in ensuring a fair tax system for Wales.
	- Richmond Council and Wandsworth Council are using Splink to match residents’ records across systems to create unified records and a single view of debt.
	- [Westmorland & Furness Council](https://www.simpson-associates.co.uk/clients/westmorland-furness-council/) used Splink to matched and de-duplicated Special Educational Needs and Disability (SEND) records across systems.  This provided a “single view of the child”, improved data quality and automation, and laid the foundation for a wider “Single View of the Customer” initiative.


=== "Public Sector (International)"

    - 🇦🇺 The Australian Bureau of Statistics (ABS) used Splink to build the 2024 National Linkage Spine underpinning the [National Disability Data Asset](https://www.abs.gov.au/about/data-services/data-integration/integrated-data/national-disability-data-asset) and will use Splink for the 2025 [Person Linkage Spine](https://www.abs.gov.au/about/data-services/data-integration/person-linkage-spine) build. They are also planning to use Splink for the Post Enumeration Survey as part of the 2026 Census quality assurance process.
	- 🇩🇪 The German Federal Statistical Office ([Destatis](https://www.destatis.de/EN/Home/_node.html)) uses Splink to conduct projects in linking register-based census data.
	- 🇪🇺 The [European Medicines Agency](https://www.ema.europa.eu/en/homepage) uses Splink to detect duplicate adverse event reports for veterinary medicines
	- 🇺🇸 The Defense Health Agency (US Department of Defense) used Splink to identify duplicated hospital records across over 200 million data points in the military hospital data system
	- 🌐 [UNHCR](https://unhcr.org) uses Splink to analyse and enhance the quality of datasets by identifying and addressing potential duplicates.
	- 🇨🇦 The Data Integration Unit at the [Ontario Ministry of Children, Community, and Social Services](https://www.ontario.ca/page/ministry-children-community-and-social-services) are using Splink as their main data-integration tool for all intra- and inter-ministerial data-linking projects.
	- 🇬🇲 Splink has been used to support the 2024 Gambian census by analysing and linking data from the census and the post-enumeration survey.
	- 🇨🇦 Environment and Climate Change Canada is a user of Splink to connect datasets from various administrative and reporting programs.
	- 🇨🇱🇬🇧 [Chilean Ministry of Health](https://www.gob.cl/en/ministries/ministry-of-health/) and [University College London](https://www.ucl.ac.uk/) have [assessed the access to immunisation programs among the migrant population](https://ijpds.org/article/view/2348)
	- 🇺🇸 [Florida Cancer Registry](https://www.floridahealth.gov/diseases-and-conditions/cancer/cancer-registry/index.html), published a [feasibility study](https://scholar.googleusercontent.com/scholar?q=cache:sADwxy-D75IJ:scholar.google.com/+splink+florida&hl=en&as_sdt=0,5) which showed Splink was faster and more accurate than alternatives
	- 🇺🇸 [Catalyst Cooperative](https://catalyst.coop)'s [Public Utility Data Liberation Project](https://github.com/catalyst-cooperative/pudl) links public financial and operational data from electric utilities for use by US climate advocates, policymakers, and researchers seeking to accelerate the transition away from fossil fuels.

=== "Academia"

	- [Stanford University](https://www.stanford.edu/) investigated the impact of [receiving government assistance has on political attitudes](https://www.cambridge.org/core/journals/american-political-science-review/article/abs/does-receiving-government-assistance-shape-political-attitudes-evidence-from-agricultural-producers/39552BC5A496EAB6CB484FCA51C6AF21)
	- Researchers from [Harvard Medical School](https://hms.harvard.edu/), [Vanderbilt University Medical Center](https://www.vumc.org/) and [Brigham and Women's Hospital](https://www.brighamandwomens.org/) published a study on [augmenting death ascertainment in electronic health records using publicly available internet media sources](https://doi.org/10.1101/2025.01.24.25321042).
	- [Bern University](https://arbor.bfh.ch/) researched how [Active Learning can be applied to Biomedical Record Linkage](https://ebooks.iospress.nl/doi/10.3233/SHTI230545)
	- [University of Pennsylvania](https://www.upenn.edu/), [Princeton](https://www.princeton.edu/), and [UC Berkeley](https://www.berkeley.edu/) researchers used Splink to link property data, voter files, and campaign donations, creating a dataset of 108M individuals to study the American voter base - see [here](https://www.cambridge.org/core/services/aop-cambridge-core/content/view/04A0071D849FBC3E1DEEB2962A6B977F/S0003055425000061a.pdf).
	- 🇱🇦 The [Shared Child Health Record](https://link.springer.com/article/10.1007/s10916-025-02260-6) project in Lao PDR used Splink to de-duplicate pediatric records in a non-Latin script context

=== "Other"
	- [Marie Curie](https://podcasts.apple.com/gb/podcast/unlocking-data-at-marie-curie/id1724979056?i=1000649964922) have used Splink to build a single customer view on fundraising data which has been a "huge success [...] the tooling is just so much better. [...] The power of being able to select, plug in, configure and train a tool versus writing code. It's just mind boggling actually."  Amongst other benefits, the system is expected to "dramatically reduce manual reporting efforts previously required". See also the blog post [here](https://esynergy.co.uk/our-work/marie-curie/).
 	- [Club Brugge](https://www.clubbrugge.be/en) uses Splink to link football players from different data providers to their own database, simplifying and reducing the need for manual linkage labor.
	- [GN Group](https://www.gn.com/) use Splink to deduplicate large volumes of customer records

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

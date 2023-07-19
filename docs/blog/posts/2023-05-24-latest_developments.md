---
date: 2022-07-14
categories:
  - Feature Updates
---

# Splink Updates - July 2023

** It has been a busy few months in the Splink team, so here is a round up of the latest features that have been released.**

Latest Splink version: [v3.9.3](https://github.com/moj-analytical-services/splink/releases/tag/v3.9.3)

<!-- more -->


## :rocket: Massive speed gains in EM training

There’s now an option to make EM training much faster - in one example we’ve seen at 1000x fold speedup.  Kudos to external contributor [@aymonwuolanne](https://github.com/moj-analytical-services/splink/pull/1369) from the Australian Bureau of Statistics! 

To make use of this, set the `estimate_without_term_frequencies` parameter to True, for example:

```py
linker.estimate_parameters_using_expectation_maximisation(..., estimate_without_term_frequencies=True)
```
## :gift: Out-of-the-box Comparisons

Splink now contains lots of new out-of-the-box comparisons for dates, names, postcodes etc. The Comparison Template Library (CTL) provides suggested settings for common types of data used in linkage models. 

For example, a Comparison for `"first_name"` can now be written as:

```py
import splink.duckdb.comparison_template_library as ctl

first_name_comparison = ctl.name_comparison("first_name")
```

Check out these new functions in the [CTL Topic Guide](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/comparison_templates.html) and [CTL Documentation](https://moj-analytical-services.github.io/splink/comparison_template_library.html?h=comp).

## :simple-adblock: Blocking Rule Library

Blocking has, historically, been a point of confusion for users so we have been working behind the scenes to make that easier! The recently launched Blocking Rules Library (BRL) provides a set of functions for defining Blocking Rules (similar to the Comparison Library functions). 

For example, a Blocking Rule for `"date_of_birth"` can now be written as:

```py
import splink.duckdb.blocking_rule_library as brl

brl.exact_match_rule("date_of_birth")
```

Check out these new functions in the [BRL Documentation](https://moj-analytical-services.github.io/splink/blocking_rule_library.html) as well as some new [Blocking Topic Guides](https://moj-analytical-services.github.io/splink/topic_guides/blocking/blocking_rules.html) to better explain what Blocking Rules are, how they are used in Splink, and how to choose them.

Keep a look out, as there is more improvements in the pipeline for Blocking in the coming months!

## :elephant: Postgres Support

With a massive thanks to external contributor [@hanselmm](https://github.com/moj-analytical-services/splink/pull/1191), Splink now supports :simple-postgresql: Postgres. To get started, check out the [Postgres Topic Guide](https://moj-analytical-services.github.io/splink/topic_guides/backends/postgres.html).

## :label: Clerical Labelling Tool (beta)

Clerical labelling is an important tool for [generating performance metrics](https://moj-analytical-services.github.io/splink/demos/tutorials/07_Quality_assurance.html) for linkage models (False Positive Rate, Recall, Precision etc.).

Splink now has a (beta) GUI for clerical labelling which produces labels in a form that can be easily ingested into Splink to generate these performance metrics.  Example here: https://robinlinacre.com/splink_example_charts/example_charts/splink3/labelling_tool_dedupe_only.html PR here: https://github.com/moj-analytical-services/splink/pull/1208, and some previous tweets:

<blockquote class="twitter-tweet"><p lang="en" dir="ltr">Draft new Splink tool to speed up manual labelling of record linkage data. Example dashboard: <a href="https://t.co/yc1yHpa90X">https://t.co/yc1yHpa90X</a> <br><br>Grateful for any feedback whilst I&#39;m still working on this, on Twitter or the draft PR: <a href="https://t.co/eXSNHHe2kc">https://t.co/eXSNHHe2kc</a><br><br>Free and open source <a href="https://t.co/MEo4DmaxO9">pic.twitter.com/MEo4DmaxO9</a></p>&mdash; Robin Linacre (@RobinLinacre) <a href="https://twitter.com/RobinLinacre/status/1651845520057421825?ref_src=twsrc%5Etfw">April 28, 2023</a></blockquote> <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>


## :books: Documentation Improvements

We have been putting a lot of effort into improving our documentation site, including launching this blog!

Some of the improvements include:

* More Topic Guides covering things such as [Record Linkage Theory](https://moj-analytical-services.github.io/splink/topic_guides/theory/record_linkage.html), [Guidance on Splink's backends](https://moj-analytical-services.github.io/splink/topic_guides/backends/backends.html) and [String Fuzzy Matching](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/choosing_comparators.html)
* A [Contributors Guide](https://moj-analytical-services.github.io/splink/CONTRIBUTING.html) to make contrbuting to Splink even easier. If you are interested in getting involved in open source, check the guide out!
* 


Thanks to everyone who filled out our [feedback survey](https://forms.gle/4S9PJgFX7opE9ggu9). If you have any more feedback or ideas for how we can make the docs better please do let us know by [raising an issue](https://github.com/moj-analytical-services/splink/issues), [starting a discussion](https://github.com/moj-analytical-services/splink/discussions) or filling out the survey.

## :bar_chart: Charts in Altair 5

Charts are now all fully-fledged Altair charts, making them much easier to work with.  

For example, a chart `c` can now be saved with:

```py
c.save(“chart.png”, scale_factor=2)
```

where `json`, `html`, `png`, `svg` and `pdf` are all supported.


## :soon: What's in the pipeline?

* :simple-adblock:   More Blocking improvements  
* :clipboard:   Settings improvements  
* :material-thumbs-up-down:   Evaluation Framework


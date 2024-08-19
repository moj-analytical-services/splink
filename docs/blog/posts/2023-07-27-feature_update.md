---
date: 2023-07-27
authors:
  - ross-k
  - robin-l
categories:
  - Feature Updates
---

# Splink Updates - July 2023

## :new: Welcome to the Splink Blog! :new:

Its hard to keep up to date with all of the new features being added to Splink, so we have launched this blog to share a round up of latest developments every few months.

So, without further ado, here are some of the highlights from the first half of 2023!

<!-- more -->

Latest Splink version: [v3.9.4](https://github.com/moj-analytical-services/splink/releases/tag/v3.9.4)


## :rocket: Massive speed gains in EM training

There’s now an option to make EM training much faster - in one example we’ve seen at [1000x fold speedup](https://github.com/moj-analytical-services/splink/pull/1369#issuecomment-1611214919).  Kudos to external contributor [@aymonwuolanne](https://github.com/moj-analytical-services/splink/pull/1369) from the Australian Bureau of Statistics!

To make use of this, set the [`estimate_without_term_frequencies`](https://moj-analytical-services.github.io/splink/linkerest.html#splink.linker.Linker.estimate_parameters_using_expectation_maximisation) parameter to True; for example:

```py
linker.estimate_parameters_using_expectation_maximisation(..., estimate_without_term_frequencies=True)
```

Note: If True, the EM algorithm ignores term frequency adjustments during the iterations. Instead, the adjustments are added once the EM algorithm has converged. This will result in slight difference in the final parameter estimations.

## :gift: Out-of-the-box Comparisons

Splink now contains lots of new out-of-the-box comparisons for dates, names, postcodes etc. The Comparison Template Library (CTL) provides suggested settings for common types of data used in linkage models.

For example, a Comparison for `"first_name"` can now be written as:

```py
import splink.duckdb.comparison_template_library as ctl

first_name_comparison = ctl.name_comparison("first_name")
```

Check out these new functions in the [Topic Guide](../../topic_guides/comparisons/out_of_the_box_comparisons.ipynb) and [Documentation](../../api_docs/comparison_library.md).

## :simple-adblock: Blocking Rule Library

Blocking has, historically, been a point of confusion for users so we have been working behind the scenes to make that easier! The recently launched Blocking Rules Library (BRL) provides a set of functions for defining Blocking Rules (similar to the Comparison Library functions).

For example, a Blocking Rule for `"date_of_birth"` can now be written as:

```py
import splink.duckdb.blocking_rule_library as brl

brl.exact_match_rule("date_of_birth")
```

**Note**: from Splink v3.9.6, `exact_match_rule` has been superseded by `block_on`. We advise using this going forward.

Check out these new functions in the [BRL Documentation](https://moj-analytical-services.github.io/splink/blocking_rule_library.html) as well as some new [Blocking Topic Guides](https://moj-analytical-services.github.io/splink/topic_guides/blocking/blocking_rules.html) to better explain what Blocking Rules are, how they are used in Splink, and how to choose them.

Keep a look out, as there are more improvements in the pipeline for Blocking in the coming months!

## :elephant: Postgres Support

With a massive thanks to external contributor [@hanslemm](https://github.com/moj-analytical-services/splink/pull/1191), Splink now supports :simple-postgresql: Postgres. To get started, check out the [Postgres Topic Guide](https://moj-analytical-services.github.io/splink/topic_guides/backends/postgres.html).

## :label: Clerical Labelling Tool (beta)

Clerical labelling is an important tool for [generating performance metrics](https://moj-analytical-services.github.io/splink/demos/tutorials/07_Quality_assurance.html) for linkage models (False Positive Rate, Recall, Precision etc.).

Splink now has a (beta) GUI for clerical labelling which produces labels in a form that can be easily ingested into Splink to generate these performance metrics. Check out the [example tool](https://robinlinacre.com/splink_example_charts/example_charts/splink3/labelling_tool_dedupe_only.html), linked [Pull Request](https://github.com/moj-analytical-services/splink/pull/1208), and some previous tweets:

<blockquote class="twitter-tweet"><p lang="en" dir="ltr">Draft new Splink tool to speed up manual labelling of record linkage data. Example dashboard: <a href="https://t.co/yc1yHpa90X">https://t.co/yc1yHpa90X</a> <br><br>Grateful for any feedback whilst I&#39;m still working on this, on Twitter or the draft PR: <a href="https://t.co/eXSNHHe2kc">https://t.co/eXSNHHe2kc</a><br><br>Free and open source <a href="https://t.co/MEo4DmaxO9">pic.twitter.com/MEo4DmaxO9</a></p>&mdash; Robin Linacre (@RobinLinacre) <a href="https://twitter.com/RobinLinacre/status/1651845520057421825?ref_src=twsrc%5Etfw">April 28, 2023</a></blockquote> <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>

This tool is still in the beta phase, so is a work in progress and subject to change based on feedback we get from users. As a result, it is not thoroughly documented at this stage. We recommend checking out the links above to see a ready-made example of the tool. However, if you would like to generate your own, [this example](https://gist.github.com/RobinL/7512b8b3b31c42b13b5a28aac5a363b4) is a good starting point.

We would love any feedback from users, so please comment on the [PR](https://github.com/moj-analytical-services/splink/pull/1208) or open a [discussion](https://github.com/moj-analytical-services/splink/discussions).


## :bar_chart: Charts in Altair 5

Charts are now all fully-fledged Altair charts, making them much easier to work with.

For example, a chart `c` can now be saved with:

```py
c.save(“chart.png”, scale_factor=2)
```

where `json`, `html`, `png`, `svg` and `pdf` are all supported.


## :octicons-duplicate-24: Reduced duplication in Comparison libraries

Historically, importing of the comparison libraries has included declaring the backend twice. For example:

```py
import splink.duckdb.duckdb_comparison_level_library as cll
```
This repetition has now been removed
```py
import splink.duckdb.comparison_level_library as cll
```
The original structure still works, but throws a warning to switch to the new version.

## :material-table: In-built datasets

When following along with the tutorial or example notebooks, one issue can be references of paths to data that does not exists on users machines. To overcome this issue, Splink now has a `splink_datasets` module which will store these datasets and make sure any users can copy and paste working code without fear of path issues. For example:

```py
from splink.datasets import splink_datasets

df = splink_datasets.fake_1000
```
returns the fake 1000 row dataset that is used in the Splink [tutorial](../../demos/tutorials/00_Tutorial_Introduction.ipynb).

For more information check out the in-built datasets [Documentation](../../api_docs/datasets.md).

## :material-regex: Regular Expressions in Comparisons

When comparing records, some columns will have a particular structure (e.g. dates, postcodes, email addresses). It can be useful to compare sections of a column entry. Splink's string comparison level functions now include a `regex_extract` to extract a portion of strings to be compared. For example, an `exact_match` comparison that compares the first section of a postcode (outcode) can be written as:

```py
import splink.duckdb.duckdb_comparison_library as cl

pc_comparison = cl.exact_match("postcode", regex_extract="^[A-Z]{1,2}")
```

Splink's string comparison level functions also now include a `valid_string_regex` parameter which sends any entries that do not conform to a specified structure to the null level. For example, a `levenshtein` comparison that ensures emails have an "@" symbol can be written as:

```py
import splink.duckdb.duckdb_comparison_library as cl

email_comparison = cl.levenshtein_at_thresholds("email", valid_string_regex="^[^@]+")
```

For more on how Regular Expressions can be used in Splink, check out the [Regex topic guide](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/regular_expressions.html#example-using-valid_string_regex).

Note: from Splink v3.9.6, `valid_string_regex` has been renamed as `valid_string_pattern`.

## :books: Documentation Improvements

We have been putting a lot of effort into improving our documentation site, including launching this blog!

Some of the improvements include:

* More Topic Guides covering things such as [Record Linkage Theory](https://moj-analytical-services.github.io/splink/topic_guides/theory/record_linkage.html), [Guidance on Splink's backends](https://moj-analytical-services.github.io/splink/topic_guides/backends/backends.html) and [String Fuzzy Matching](https://moj-analytical-services.github.io/splink/topic_guides/comparisons/choosing_comparators.html).
* A [Contributors Guide](../../dev_guides/CONTRIBUTING.md) to make contributing to Splink even easier. If you are interested in getting involved in open source, check the guide out!
* Adding tables to the Comparison [libraries documentation](../../api_docs/comparison_level_library.md) to show the functions available for each SQL backend.

Thanks to everyone who filled out our [feedback survey](https://forms.gle/4S9PJgFX7opE9ggu9). If you have any more feedback or ideas for how we can make the docs better please do let us know by [raising an issue](https://github.com/moj-analytical-services/splink/issues), [starting a discussion](https://github.com/moj-analytical-services/splink/discussions) or filling out the survey.

## :soon: What's in the pipeline?

* :simple-adblock:   More Blocking improvements
* :clipboard:   Settings dictionary improvements
* :material-thumbs-up-down:   More guidance on how to evaluate Splink models and linkages


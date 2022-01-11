# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0]

### Added

- `sql_expr` now added to tooltips on bayes factor chart, displaying the SQL expression for each comparison level
- Warnings to the user if they don't include a null level in their case expression, custom columns is different to cols used in case expression,
- `splink` now parses `case_expression` to auto-populate `num_levels` or `col_name` or `custom_columns_used`. The user may still provide this information, but is no longer required to.

Note that Splink now has a depedency on `sqlglot`, a no-dependency SQL parser.

## [2.0.4]

### Added

- Add function to analyse blocking rules `from splink.analyse_blocking_rule import analyse_blocking_rule` in https://github.com/moj-analytical-services/splink/pull/260

**Full Changelog**: https://github.com/moj-analytical-services/splink/compare/v2.0.3...v2.0.4

## [2.0.3]

### Added

- Gamma distribution chart by @RobinL in https://github.com/moj-analytical-services/splink/pull/246

**Full Changelog**: https://github.com/moj-analytical-services/splink/compare/v2.0.2...v2.0.3

## [2.0.2]

### Added

- Add function to compute m values from labelled data by @RobinL in https://github.com/moj-analytical-services/splink/pull/248

## [2.0.1]

### Added

- Add function that outputs the full path to the similarity jar by @RobinL in https://github.com/moj-analytical-services/splink/pull/237

### Changed

- Allow match weight to be used in the diagnostic histogram by @RobinL in https://github.com/moj-analytical-services/splink/pull/239

## [2.0.0]

### Changed

- Term frequency adjustments are now calculated directly from a term freqency lookup table making them more accurate
- Term frequency adjustments are now part of the iterative EM estimation step, improving convergence
- All internal calculations are changed to use bayes factors (match weights) rather than probabilities to make the maths simpler

### Added

- Splink now outputs `match_weight`, the log2(Bayes Factor) of the match score.
- New `splink.charts.save_offline_chart` function that produces charts that work in airgapped environments with no internet connection
- New `splink.cluster.clusters_at_thresholds` function that clusters are one or more match thresholds
- The `splink.truth.roc_chart` function now allows several ROCS to be plotted on a single chart, to compare the accuracy of different models
- Splink now includes an slower Python implementation of jaro_winkler, in case users are having trouble with the string similarity jar

### Removed

- Since term frequency adjustments are no longer an ex-post step, there's no longer a need for them to be calculated separately. Splink therefore no longer outputs `tf_adjusted_match_prob`. Instead, TF adjustments are included within `match_probability`

## [1.0.5]

### Fixed

- Bug that meant default numerical case statements were not available. See [here](https://github.com/moj-analytical-services/splink/issues/189). Thanks to [geobetts](https://github.com/geobetts)

### Changed

- `m` and `u` probabilities are now reset to `None` rather than `0` in EM iteration when they cannot be estimated
- Now use `_repr_pretty_` so that objects display nicely in Jupyter Lab rather than `__repr__`, which had been interfering with the interpretatino of stack trace errors

## [1.0.3] - 2020-02-04

- Bug whereby Splink lowercased case expressions, see [here](https://github.com/moj-analytical-services/splink/issues/174)

## [1.0.2] - 2020-02-02

### Changed

- Improve estimate comparison charts, including tooltips and better labels

## [1.0.1] - 2020-01-31

### Changed

- Added mousewheel zoom to bayes factor chart
- Added mousewheel zoom to splink score histogram
- Update estimate comparison chart to use different shapes for different estimates, making it possible to distinguish overlapping symbols

### Fixed

- m and u history charts now display barchart correctly

## [1.0.0] - 2020-01-20

### Added

- Charts now feature improved tooltips, and have a cleaner appearance. Many are now zoomable
- Charts now display better in Jupyter Lab, especially the html file produced by `all_charts_write_html_file()`
- `m` and `u` probabilities charts can now be produced from `Settings` objects
- The user can now combine settings objects using `ModelCombiner from splink.combine_models`

### Changed

A number of **backwards incompatible** changes have been made for Splink 1.0.

- The main `Splink` API is different. Instead of `Splink(...,df=df)` for dedupe and `Splink(...,df_l=df_l,df_r=df_r)` for linking, the user provides an agument `df_or_dfs`, which is either a single DataFrame or a list of DataFrames. This allows linking n>2 datasets.
- When linking multiple dataframes, the user must now include a `source_dataset` column (default name `source_dataset`, configurable via `source_dataset_column_name` in the settings dict)
- The `Params` class is now called `Model` in the `model.py` module.
- The on-disk (json) format of the `Model` object has changed and is incompatible with `Params`
- The new `Model` class now uses the same representation for parameters as the Settings object, reducing duplicate code. Internal functions now have `settings` or `model` as function arguments, never both.
- Vega lite chart definitions now stored in json files in splink/files/chart_defs
- All case statement generation functions are now consistently named, with all names starting `sql_gen_case_stmt_`
- Fixed `case_statements.sql_gen_case_smnt_strict_equality_2` which previously behaved differently to all other case functions
- All case statements now have a default threshold of exact equality on their top gamma level

### Fixed

### Removed

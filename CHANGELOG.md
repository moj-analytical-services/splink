# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.5]

### Fixed

- Bug that meant default numerical case statements were not available.  See [here](https://github.com/moj-analytical-services/splink/issues/189).  Thanks to [geobetts](https://github.com/geobetts)

### Changed

- `m` and `u` probabilities are now reset to `None` rather than `0`  in EM iteration when they cannot be estimated


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
- Charts now feature improved tooltips, and have a cleaner appearance.  Many are now zoomable
- Charts now display better in Jupyter Lab, especially the html file produced by `all_charts_write_html_file()`
- `m` and `u` probabilities charts can now be produced from `Settings` objects
- The user can now combine settings objects using `ModelCombiner from splink.combine_models`
### Changed


A number of **backwards incompatible** changes have been made for Splink 1.0.

- The main `Splink` API is different.  Instead of `Splink(...,df=df)` for dedupe and `Splink(...,df_l=df_l,df_r=df_r)` for linking, the user provides an agument `df_or_dfs`, which is either a single DataFrame or a list of DataFrames.  This allows linking n>2 datasets.
- When linking multiple dataframes, the user must now include a `source_dataset` column (default name `source_dataset`, configurable via `source_dataset_column_name` in the settings dict)
- The `Params` class is now called `Model` in the `model.py` module.
- The on-disk (json) format of the `Model` object has changed and is incompatible with `Params`
- The new `Model` class now uses the same representation for parameters as the Settings object, reducing duplicate code.  Internal functions now have `settings` or `model` as function arguments, never both.
- Vega lite chart definitions now stored in json files in splink/files/chart_defs
- All case statement generation functions are now consistently named, with all names starting `sql_gen_case_stmt_`
- Fixed `case_statements.sql_gen_case_smnt_strict_equality_2` which previously behaved differently to all other case functions
- All case statements now have a default threshold of exact equality on their top gamma level



### Fixed

### Removed







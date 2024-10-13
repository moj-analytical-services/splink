# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [4.0.4] - 2024-10-13

### Added

- `cluster_pairwise_predictions_at_multiple_thresholds` to more efficiently cluster at multiple thresholds ([#2437](https://github.com/moj-analytical-services/splink/pull/2437))

### Fixed

- Fixed issue with `profile_columns` using latest Altair version ([#2466](https://github.com/moj-analytical-services/splink/pull/2466))

## [4.0.3] - 2024-09-19

### Added

- Cluster without linker by @RobinL in https://github.com/moj-analytical-services/splink/pull/2412
- Better autocomplete for dataframes by @RobinL in https://github.com/moj-analytical-services/splink/pull/2434


## [4.0.2] - 2024-09-19

### Added

- Match weight and m and u probabilities charts now have improved tooltips ([#2392](https://github.com/moj-analytical-services/splink/pull/2392))
- Added new `AbsoluteDifferenceLevel` comparison level for numerical columns ([#2398](https://github.com/moj-analytical-services/splink/pull/2398))
- Added new `CosineSimilarityLevel` and `CosineSimilarityAtThresholds` for comparing array columns using cosine similarity ([#2405](https://github.com/moj-analytical-services/splink/pull/2405))
- Added new `ArraySubsetLevel` for comparing array columns ([#2416](https://github.com/moj-analytical-services/splink/pull/2416))

### Fixed

- Fixed issue where `ColumnsReversedLevel` required equality on both columns ([#2395](https://github.com/moj-analytical-services/splink/pull/2395))

## [4.0.1] - 2024-09-06

### Added

- When using DuckDB, you can now pass `duckdb.DuckDBPyRelation`s as input tables to the `Linker` ([#2375](https://github.com/moj-analytical-services/splink/pull/2375))
- It's now possible to fix values for `m` and `u` probabilities in the settings such that they are not updated/changed during training.  ([#2379](https://github.com/moj-analytical-services/splink/pull/2379))
- All charts can now be returned as vega lite spec dictionaries ([#2361](https://github.com/moj-analytical-services/splink/pull/2361))


### Fixed

- Completeness chart now works correctly with indexed columns in spark ([#2309](https://github.com/moj-analytical-services/splink/pull/2309))
- Completeness chart works even if you have a `source_dataset` column ([#2323](https://github.com/moj-analytical-services/splink/pull/2323))
- `SQLiteAPI` can now be instantiated without error when opting not to register custom UDFs  ([#2342](https://github.com/moj-analytical-services/splink/pull/2342))
- Splink now runs properly when working in read-only filesystems ([#2357](https://github.com/moj-analytical-services/splink/pull/2357))
- Infinite Bayes factor no longer causes SQL error in `Spark` ([#2372](https://github.com/moj-analytical-services/splink/pull/2372))
- `splink_datasets` is now functional in read-only filesystems ([#2378](https://github.com/moj-analytical-services/splink/pull/2378))


## [4.0.0] - 2024-07-24

Major release - see our [blog](https://moj-analytical-services.github.io/splink/blog/2024/07/24/splink-400-released.html) for what's changed


## [3.9.15] - 2024-06-18

### Fixed

- Activates `higher_is_more_similar` kwarg in `cl.distance_function_at_thresholds`, see [here](https://github.com/moj-analytical-services/splink/pull/2116)
- `linker.save_model_to_json()` now correctly serialises `tf_minimum_u_value` and reloads. See [here](https://github.com/moj-analytical-services/splink/pull/2122).
- Performance improvements on code geenration, see [here](https://github.com/moj-analytical-services/splink/pull/2212)

## [3.9.14] - 2024-03-25

### Fixed

- `IndexError: List index out of range` error due to API change `SQLGlot>=23.0.0`, see [here](https://github.com/moj-analytical-services/splink/pull/2079)

### Added

- Ability to override detection of exact match level for tf adjustments. See [here](https://gist.github.com/RobinL/6e11c04aa1204ac3e7452eddd778ab4f) for example.
- Added method for computing graph metrics ([#2027](https://github.com/moj-analytical-services/splink/pull/2027))

## [3.9.13] - 2024-03-04

- Support for Databricks Runtime 13.x+

### Fixed

- Bug that prevented `sqlglot <= 17.0.0` from working properly ([#1996](https://github.com/moj-analytical-services/splink/pull/1996))
- Fixed issues relating to duckdb 0.10.1 ([#1999](https://github.com/moj-analytical-services/splink/pull/1999))
- Update sqlglot compatibility to support latest version ([#1998](https://github.com/moj-analytical-services/splink/pull/1998))

## [3.9.12] - 2024-01-30

### Fixed

- Support `sqlalchemy >= 2.0.0` ([#1908](https://github.com/moj-analytical-services/splink/pull/1908))

## [3.9.11] - 2024-01-17

### Added

- Ability to block on array columns by specifying `arrays_to_explode` in your blocking rule. ([#1692](https://github.com/moj-analytical-services/splink/pull/1692))
- Added ability to sample by density in cluster studio by @zslade in ([#1754](https://github.com/moj-analytical-services/splink/pull/1754))

### Changed

- Splink now fully parallelises data linkage when using DuckDB ([#1796](https://github.com/moj-analytical-services/splink/pull/1796))

### Fixed

- Allow salting in EM training ([#1832](https://github.com/moj-analytical-services/splink/pull/1832))

## [3.9.10] - 2023-12-07

### Changed

- Remove unused code from Athena linker ([#1775](https://github.com/moj-analytical-services/splink/pull/1775))
- Add argument for `register_udfs_automatically` ([#1774](https://github.com/moj-analytical-services/splink/pull/1774))

### Fixed

- Fixed issue with `_source_dataset_col` and `_source_dataset_input_column` ([#1731](https://github.com/moj-analytical-services/splink/pull/1731))
- Delete cached tables before resetting the cache ([#1752](https://github.com/moj-analytical-services/splink/pull/1752)

## [3.9.9] - 2023-11-14

### Changed

- Upgraded [sqlglot](https://github.com/tobymao/sqlglot) to versions >= 13.0.0 ([#1642](https://github.com/moj-analytical-services/splink/pull/1642))
- Improved logging output from settings validation ([#1636](https://github.com/moj-analytical-services/splink/pull/1636)) and corresponding documentation ([#1674](https://github.com/moj-analytical-services/splink/pull/1674))
- Emit a warning when using a default (i.e. non-trained) value for `probability_two_random_records_match` ([#1653](https://github.com/moj-analytical-services/splink/pull/1653))

### Fixed

- Fixed issue causing occasional SQL errors with certain database and catalog combinations ([#1558](https://github.com/moj-analytical-services/splink/pull/1558))
- Fixed issue where comparison vector grid not synced with corresponding histogram values in comparison viewer dashboard ([#1652](https://github.com/moj-analytical-services/splink/pull/1652))
- Fixed issue where composing null levels would mistakenly sometimes result in a non-null level ([#1672](https://github.com/moj-analytical-services/splink/pull/1672))
- Labelling tool correctly works even when offline ([#1646](https://github.com/moj-analytical-services/splink/pull/1646))
- Explicitly cast values when using the postgres linker ([#1693](https://github.com/moj-analytical-services/splink/pull/1693))
- Fixed issue where parameters to `completeness_chart` were not being applied ([#1662](https://github.com/moj-analytical-services/splink/pull/1662))
- Fixed issue passing boto3_session into the Athena linker ([#1733](https://github.com/moj-analytical-services/splink/pull/1733/files))

## [3.9.8] - 2023-10-05

### Added

- Added ability to delete tables with Spark when working in Databricks ([#1526](https://github.com/moj-analytical-services/splink/pull/1526))

### Changed

- Re-added support for python 3.7 (specifically >= 3.7.1) and adjusted dependencies in this case ([#1622](https://github.com/moj-analytical-services/splink/pull/1622))

### Fixed

- Fix behaviour where using `to_csv` with Spark backend wouldn't overwrite files even when instructed to ([#1635](https://github.com/moj-analytical-services/splink/pull/1635))
- Corrected path for Spark `.jar` file containing UDFs to work correctly for Spark < 3.0 ([#1622](https://github.com/moj-analytical-services/splink/pull/1622))
- Spark UDF `damerau_levensthein` is now only registered for Spark >= 3.0, as it is not compatible with earlier versions ([#1622](https://github.com/moj-analytical-services/splink/pull/1622))

[unreleased]: https://github.com/moj-analytical-services/splink/compare/4.0.0...HEAD
[4.0.0]: https://github.com/moj-analytical-services/splink/compare/3.9.15...4.0.0
[3.9.15]: https://github.com/moj-analytical-services/splink/compare/3.9.14...3.9.15
[3.9.14]: https://github.com/moj-analytical-services/splink/compare/3.9.13...3.9.14
[3.9.13]: https://github.com/moj-analytical-services/splink/compare/3.9.12...3.9.13
[3.9.12]: https://github.com/moj-analytical-services/splink/compare/3.9.11...3.9.12
[3.9.11]: https://github.com/moj-analytical-services/splink/compare/3.9.10...3.9.11
[3.9.10]: https://github.com/moj-analytical-services/splink/compare/v3.9.9...3.9.10
[3.9.9]: https://github.com/moj-analytical-services/splink/compare/v3.9.8...3.9.9
[3.9.8]: https://github.com/moj-analytical-services/splink/compare/v3.9.7...v3.9.8

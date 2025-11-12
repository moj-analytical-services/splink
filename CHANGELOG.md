# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [4.0.11] - 2025-11-12

### Added
* Improve clustering performance by @aymonwuolanne in https://github.com/moj-analytical-services/splink/pull/2800
* Improve waterfall generation performance by @RobinL in https://github.com/moj-analytical-services/splink/pull/2816

## [4.0.10] - 2025-11-03

### Added

- Spark 4 compatible versions of UDFs [#2802](https://github.com/moj-analytical-services/splink/pull/2802)

### Changed

- Changes to `debug_mode` so that it better aligns with ordinary execution ([#2789](https://github.com/moj-analytical-services/splink/pull/2789))

### Fixed

- Adjusted SQL used in `cluster_pairwise_predictions_at_thresholds` to help Spark optimise it better ([#2766](https://github.com/moj-analytical-services/splink/pull/2766))
- Guard against issue where calculated Bayes factor becomes zero, leading to logarithm domain errors ([#2758](https://github.com/moj-analytical-services/splink/pull/2758))
- Fix count of generated comparisons when using exploding blocking rules ([#2778](https://github.com/moj-analytical-services/splink/pull/2778))
- Allow failed date-parsing in Spark 4 to fall through as `NULL` ([#2805](https://github.com/moj-analytical-services/splink/pull/2805))

### Deprecated

- Deprecated support for python `3.9.x` following end of support for that minor version ([#2797](https://github.com/moj-analytical-services/splink/pull/2797))

### Removed

- Removed no-longer-used function `validate_settings_against_schema` and corresponding dependency on `jsonschema` ([#2798](https://github.com/moj-analytical-services/splink/pull/2798))

## [4.0.9] - 2025-09-24

### Fixed

- Fix issue where exact match levels are not correctly identified with newer `sqlglot` versions [#2780](https://github.com/moj-analytical-services/splink/pull/2780)
- Fix issue with l/r transformations in newer `sqlglot` versions for certain expressions [#2780](https://github.com/moj-analytical-services/splink/pull/2780)

## [4.0.8] - 2025-06-04

### Fixed

- Fix bug where u-sampling with a seed fails with custom `unique_id_column_name` [#2659](https://github.com/moj-analytical-services/splink/pull/2659)

### Changed

- New version of Spark udf `.jar` file, which has updated dependencies [#2679](https://github.com/moj-analytical-services/splink/pull/2679)

## [4.0.7] - 2025-03-04

### Added

- Support for 'one to one' linking and clustering (allowing the user to force clusters to contain at most one record from given `source_dataset`s) in [#2578](https://github.com/moj-analytical-services/splink/pull/2578/)
- `ColumnExpression` now supports accessing first or last element of an array column via method `access_extreme_array_element()` ([#2585](https://github.com/moj-analytical-services/splink/pull/2585)), or converting string literals to `NULL` via `nullif()` ([#2586](https://github.com/moj-analytical-services/splink/pull/2586))
- `PairwiseStringDistanceFunction` now works with `spark` backend ([#2546](https://github.com/moj-analytical-services/splink/pull/2546))
- `linker.clustering.compute_graph_metrics()` now also computes node centrality ([#2618](https://github.com/moj-analytical-services/splink/pull/2618))

### Fixed

- Fixed issue where `estimate_u_using_random_sampling()` could give different answers between runs even when a `seed` is set ([#2642](https://github.com/moj-analytical-services/splink/pull/2642))
- Fixed issue where `compare_records` could return the wrong cached SQL when more than one model in memory ([#2589](https://github.com/moj-analytical-services/splink/pull/2589))
- `SparkAPI` now correctly handles case where database is not a valid SQL identifier ([#2577](https://github.com/moj-analytical-services/splink/pull/2577))

### Deprecated

- Deprecated support for python `3.8.x` following end of support for that minor version ([#2520](https://github.com/moj-analytical-services/splink/pull/2520))

### Changed

- Upgraded Vega to 5.31 ([#2599](https://github.com/moj-analytical-services/splink/issues/2599))

## [4.0.6] - 2024-12-05

### Added
- Added new `PairwiseStringDistanceFunctionLevel` and `PairwiseStringDistanceFunctionAtThresholds`
  for comparing array columns using a string similarity on each pair of values ([#2517](https://github.com/moj-analytical-services/splink/pull/2517))
- Compare two records now allows typed inputs, not just dict ([#2498](https://github.com/moj-analytical-services/splink/pull/2498))
- Clustering allows match weight args not just match probability ([#2454](https://github.com/moj-analytical-services/splink/pull/2454))

### Fixed

- Various bugfixes for `debug_mode` ([#2481](https://github.com/moj-analytical-services/splink/pull/2481))
- Clustering still works in DuckDB even if no edges are available ([#2510](https://github.com/moj-analytical-services/splink/pull/2510))

## [4.0.5] - 2024-11-06

### Fixed

- Dataframes to be registered when using `compare_two_records`, to avoid problems with data typing (because the input data can have an explicit schema) ([#2493](https://github.com/moj-analytical-services/splink/pull/2493))

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

[Unreleased]: https://github.com/moj-analytical-services/splink/compare/v4.0.10...HEAD
[4.0.10]: https://github.com/moj-analytical-services/splink/compare/v4.0.9...v4.0.10
[4.0.9]: https://github.com/moj-analytical-services/splink/compare/v4.0.8...v4.0.9
[4.0.8]: https://github.com/moj-analytical-services/splink/compare/v4.0.7...v4.0.8
[4.0.7]: https://github.com/moj-analytical-services/splink/compare/v4.0.6...v4.0.7
[4.0.6]: https://github.com/moj-analytical-services/splink/compare/v4.0.5...v4.0.6
[4.0.5]: https://github.com/moj-analytical-services/splink/compare/v4.0.4...v4.0.5
[4.0.4]: https://github.com/moj-analytical-services/splink/compare/v4.0.3...v4.0.4
[4.0.3]: https://github.com/moj-analytical-services/splink/compare/v4.0.2...v4.0.3
[4.0.2]: https://github.com/moj-analytical-services/splink/compare/v4.0.1...v4.0.2
[4.0.1]: https://github.com/moj-analytical-services/splink/compare/v4.0.0...v4.0.1
[4.0.0]: https://github.com/moj-analytical-services/splink/compare/v3.9.15...v4.0.0
[3.9.15]: https://github.com/moj-analytical-services/splink/compare/v3.9.14...v3.9.15
[3.9.14]: https://github.com/moj-analytical-services/splink/compare/v3.9.13...v3.9.14
[3.9.13]: https://github.com/moj-analytical-services/splink/compare/v3.9.12...v3.9.13
[3.9.12]: https://github.com/moj-analytical-services/splink/compare/v3.9.11...v3.9.12
[3.9.11]: https://github.com/moj-analytical-services/splink/compare/v3.9.10...v3.9.11
[3.9.10]: https://github.com/moj-analytical-services/splink/compare/v3.9.9...v3.9.10
[3.9.9]: https://github.com/moj-analytical-services/splink/compare/v3.9.8...v3.9.9
[3.9.8]: https://github.com/moj-analytical-services/splink/compare/v3.9.7...v3.9.8

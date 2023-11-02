# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Upgraded [sqlglot](https://github.com/tobymao/sqlglot) to versions >= 13.0.0 (#1642)
- Improved logging output from settings validation (#1636)
- Emit a warning when using a default (i.e. non-trained) value for `probability_two_random_records_match` (#1653)

### Fixed

- Fixed issue causing occasional SQL errors with certain database and catalog combinations (#1558)
- Fixed issue where comparison vector grid not synced with corresponding histogram values in comparison viewer dashboard (#1652)
- Fixed issue where composing null levels would mistakenly sometimes result in a non-null level (#1672)
- Labelling tool correctly works even when offline (#1646)

[unreleased]: https://github.com/moj-analytical-services/splink/compare/v3.9.8...HEAD

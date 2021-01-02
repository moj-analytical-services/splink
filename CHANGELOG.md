# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- MAKE SURE CASE STATEMENTS ARE CONSISTENTLY NAMED

## [1.0.0] - 2017-06-20
### Added
- Charts now feature improved tooltips, and have a cleaner appearance
- Charts now display better in Jupyter Lab, especially the html file produced by `all_charts_write_html_file()`
- `m` and `u` probabilities charts can now be produced from `Settings` objects


### Changed
- The on-disk (json) format of the Params object has changed and is incompatible with previous versions
- Params class now uses the same representation for parameters as the Settings object, reducing duplicate code.  Internal functions now have `settings` or `params` as function arguments, never both.
- Vega lite chart definitions now stored in json files in splink/files/chart_defs
- All case statement generation functions are now consistently named, with all names starting `sql_gen_case_stmt_`
- Fixed `case_statements.sql_gen_case_smnt_strict_equality_2` which previously behaved differently to all other case functions
-

### Fixed

### Removed







---
date: 2026-09-17
authors:
  - robin-l
  - andy-b
categories:
  - Feature Updates
---

# Splink 5.0.0 released


[Splink](../../index.md) is a free and open source library for record linkage and deduplication, capable of processing 1 billion records or more. It is [widely used](../../index.md#use-cases) in government, academia and the private sector and has been downloaded over 20 million times.

We're pleased to release Splink version 5, which is more scalable, faster to train models, lighter to install, and easier to run in production than Splink 4.

## Backwards compatibility

There has been no change to the statistical model. Models trained in Splink 4 produce the same results in Splink 5, and the model serialisation format is unchanged, so models saved from Splink 4 in `.json` format can be loaded directly into Splink 5.

However, Splink 5 syntax is not fully backwards compatible and Splink 4 scripts will need small adjustments to work in Splink 5. Most changes are mechanical, and the core workflow - train a model, predict, cluster - is unchanged.

## Main enhancements


- **Built for very large jobs, with progress updates.** `predict()` now supports splitting the work into chunks.  This allows you to quickly compute a slice of the overall results table, and also allows `predict()` to log progress updates and estimated time to completion.  This also enables large jobs to be split across multiple machines when using DuckDB.

- **Complete large jobs faster.**  In our tests using the forthcoming DuckDB 2.0 engine, a 1bn row, 10bn comparison `predict()` job took 8.5 minutes to complete on a 192vCPU EC2 instance.  Significantly larger jobs are possible with chunking.

- **Faster and simpler training.**
    - When using a large sample size, `estimate_u_using_random_sampling()` is much faster thanks to chunked processing with early stopping once every comparison level has enough observations.
    - EM training using `estimate_parameters_using_expectation_maximisation()` gains a `max_pairs` cap so a loose training rule can be kept while capping the work it generates.
    - `estimate_probability_two_random_records_match()` has a `record_sample_proportion` argument to estimate from a sample of records rather than the full dataset.

- **Faster, easier blocking analysis.** Comparison counts are now estimated from a record sample by default, making blocking-rule design much faster on large data. Exact counts remain available with `record_sample_proportion=1.0`.  In addition to standalone functions, blocking analysis is now available on the `linker` object for convenience.

- **Fewer dependencies for simpler and safer installs.** Splink now depends on only `sqlglot` and `duckdb`, which themselves have no dependencies.  Pandas, NumPy, Altair and Jinja2 are now optional. This makes Splink quicker and easier to install, reduces dependency conflicts, and substantially shrinks its software supply chain surface.

- **Incremental linkage is more cleanly supported.** If you have already linked a large dataset and receive some new records, it's common to want to create only the new pairwise comparisons, avoiding the need to re-link the entire dataset. This can now be achieved using the new `predict_within()` and `predict_between()` API. This is a more flexible and robust replacement for the previous `find_matches_to_new_records()` function.

## Smaller enhancements

Some highlights of other improvements:

- **A clearer input contract.** Inputs are now registered as `SplinkDataFrame`s before being passed to the `Linker`, using `db_api.register(df, dataset_display_name="...")`. The `db_api=` argument has been removed from the `Linker` - it is derived from the registered data. Source-dataset names for `link_only` / `link_and_dedupe` are set explicitly at registration rather than inferred from positional ordering.

- **Direct Parquet materialisation.** DuckDB can now materialise intermediate and final results directly to Parquet, helping large jobs reduce memory pressure and making it easier to work with outputs that are too large to keep in memory.

- **More Pythonic logging.** Splink no longer configures Python's root logger, making it easier to embed in larger applications. New `VERBOSE`, `DEBUG`, `PIPELINE` and `SQL` logging levels give finer control over how much detail is shown.

- **Richer outputs.** `SplinkDataFrame` now exposes `as_record_list()`, `as_dict()`, `as_pyarrow_table()`. `SplinkDataFrame`s now have a `query_sql()` method.

- **A reworked pairwise scoring API.** `compare_two_records()` is replaced by `score_pair()` (one explicit pair) and `score_pairs()` (Cartesian product, no blocking).

- **Match weights instead of Bayes factors.**  Output tables now contain match weights rather than Bayes factors, with column prefixes changing from `bf_` to `mw_`. This makes results easier to interpret and the algorithms more numerically stable.

- **SQL pipeline profiling.** `DuckDBAPIWithProfiling` and `SparkAPIWithProfiling` are drop-in replacements that write detailed per-query profiles to disk, making it much easier to find the expensive stage of a job.

- **Cleaner SQL**. The SQL generated by Splink is now easier to read.  View it by setting logging to 'SQL' level in `Linker(df_sdf, settings, log_level="SQL")`


## Updating Splink 4 code

Conceptually, there are no major changes in Splink 5. Splink 5 code follows the same steps as Splink 4, uses the same core estimation and prediction routines, and produces the same results for the same settings.

Minor changes to syntax are required to upgrade Splink 4 code to Splink 5.  You can find an LLM prompt that should help you automatically upgrade any Splink 4 scripts to Splink 5 [here](https://gist.github.com/RobinL/c6d56a27d8f83c40b6b09643c0fa5d14#appendix-llm-agent-prompt-for-upgrading-splink-4-code-to-splink-5).
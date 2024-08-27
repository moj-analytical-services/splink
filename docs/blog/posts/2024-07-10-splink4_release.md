---
date: 2024-07-24
authors:
  - robin-l
categories:
  - Feature Updates
---

# Splink 4.0.0 released

We're pleased to release Splink 4, which is more scalable and easier to use than Splink 3.

For the uninitiated, [Splink](../../index.md) is a free and open source library for record linkage and deduplication at scale, capable of deduplicating 100 million records+, that is [widely used](../../index.md#use-cases) and has been downloaded over 8 million times.

Version 4 is recommended to all new users.   For existing users, there has been no change to the statistical methodology. Version 3 and 4 will give the same results, so there's no urgency to upgrade existing pipelines.

<!-- more -->

The improvements we've made to the user experience mean that Splink 4 syntax is not backwards compatible, so Splink 3 scripts will need to be adjusted to work in Splink 4.  However, the model serialisation format is unchanged, so models saved from Splink 3 in `.json` format can be imported into Splink 4.

To get started quickly with Splink 4, checkout the [examples](../../demos/examples/examples_index.md).  You can see how things have changed by comparing them to the [Splink 3 examples](https://moj-analytical-services.github.io/splink3_legacy_docs/demos/examples/examples_index.html), or see the screenshot at the bottom of this post.

## Main enhancements

- **User Experience**:  We have revamped all aspects of the user-facing API.  Functionality is easier to find, better named and better organised.

- **Faster and more scalable**  Our testing suggests that the internal changes have made Splink 4 significantly more scalable. Our testing also suggests Splink 4 is faster than Splink 3 for many workloads.  This is in addition to [dramatic speedups](https://github.com/moj-analytical-services/splink/pull/1796) that were integrated into Splink 3 in January, meaning Splink is now 5x faster for a typical workload on a modern laptop than it was in November 2023.  We welcome any feedback from users about speed and scalability, as it's hard for us to test the full range of scenarios.

- **Improved backend code quality** The Splink 4 codebase represents a big refactor to focus on code quality.  It should now be easier to contribute, and quicker and easier for the team to fix bugs.

- **Autocomplete everywhere**: All functions, most notably the settings object, have been rewritten to ensure autocomplete (IntelliSense/code completion) works.  This means you no longer need to remember the specific name of the wide range of configuration options - a key like `blocking_rules_to_generate_predictions` will autocomplete.  Where settings such as `link_type` have a predefined list of valid options, these will also autocomplete.

  ![Autocomplete example](https://github.com/user-attachments/assets/305b53ee-d11a-4104-b45f-e5b96db3c973)

## Smaller enhancements

Some highlights of other smaller improvements:

- **Linker functionality is now organised into namespaces**.  In Splink 3, a very large number of functions were available on the `linker` object, making it hard to find and remember what functionality exists.  In Splink 4, functions are available in namespaces such as `linker.training` and `linker.inference`.  Documentation [here](../../api_docs/api_docs_index.md).

- **Blocking analysis**.  The new blocking functions at `splink.blocking` include routines to ensure users don't accidentally run code that generates so many comparisons it never completes.  Blocking analysis is also much faster.  See the [blocking tutorial](../../demos/tutorials/03_Blocking.ipynb) for more.

- **Switch between dialects more easily**.  The backend SQL dialect (DuckDB, Spark etc.) is now imported using the relevant database API.  This is passed into Splink functions (such as creation of the linker), meaning that switching between dialects is now a case of importing a different database API, no other code needs to change. For example, compare the [DuckDB](../../demos/examples/duckdb/deduplicate_50k_synthetic.ipynb) and [SQLite](../../demos/examples/sqlite/deduplicate_50k_synthetic.ipynb) examples.

- **Exploratory analysis no longer needs a linker**.  Exploratory analysis that is typically conducted before starting data linking can now be done in isolation, without the user needing to configure a linker. Exploratory analysis is now available at `splink.exploratory`.  Similarly, blocking can be done without a linker using the functions at `splink.blocking`.

- **Enhancements to API documentation**. Now that the codebase is better organised, it's made it much easier provide high quality API documentation - the new pages are [here](../../api_docs/api_docs_index.md).


## Updating Splink 3 code

Conceptually, there are no major changes in Splink 4. Splink 4 code follows the same steps as Splink 3.  The same core estimation and prediction routines are used.  Splink 4 code that uses the same settings will produce the same results (predictions) as Splink 3.

That said, there have been significant changes to the syntax and a reorganisation of functions.

For users wishing to familiarise themselves with Splink 4, we recommend the easiest way is to compare and contrast the new [examples](../../demos/examples/examples_index.md) with their [Splink 3 equivalents](https://moj-analytical-services.github.io/splink3_legacy_docs/demos/examples/examples_index.html).

You may also find the following screenshot useful, which shows the diff of a fairly standard Splink 3 workflow that has been rewritten in Splink 4.

![image](https://github.com/user-attachments/assets/7fe7c9e7-1a22-4744-a5ad-281d540a8deb)

You can find the corresponding code [here](https://github.com/RobinL/temp_3_to_4/pull/1/files).

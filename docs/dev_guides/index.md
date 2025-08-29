---
hide:
  - toc
---

# Contributing to Splink



Thank you for your interest in contributing to Splink! If this is your first time working with Splink, check our [Contributors Guide](./CONTRIBUTING.md).

When making changes to Splink, there are a number of common operations that developers need to perform. The guides below lay out some of these common operations, and provides scripts to automate these processes. These include:

* [Developer Quickstart](./changing_splink/development_quickstart.md) - to get contributors up and running.
* [Linting and Formatting](./changing_splink/lint_and_format.md) - to ensure consistent code style and to reformat code, where possible.
* [Testing](./changing_splink/testing.md) - to ensure all of the codebase is performing as intended.
* [Building the Documentation locally](./changing_splink/contributing_to_docs.md) - to test any changes to the docs site render correctly.
* [Releasing a new package version](./changing_splink/releases.md) - to walk-through the release process for new versions of Splink. This generally happens every 2 weeks, or in the case of an urgent bug fix.
* [Contributing to the Splink Blog](./changing_splink/blog_posts.md) - to walk through the process of adding a post to the Splink blog.

## How Splink works

Splink is quite a large, complex codebase. The guides in this section lay out some of the key structures and key areas within the Splink codebase. These include:

* [Understanding and Debugging Splink](./debug_modes.md) - demonstrates several ways of understanding how Splink code is running under the hood. This includes Splink's debug mode and logging.
* [Transpilation using SQLGlot](./transpilation.md) - demonstrates how Splink translates SQL in order to be compatible with multiple SQL engines using the SQLGlot package.
* [Performance and caching](./caching.md) - demonstrates how pipelining and caching is used to make Splink run more efficiently.
* [Charts](./charts/understanding_and_editing_charts.md) - demonstrates how charts are built in Splink, including how to add new charts and edit existing charts.
* [User-Defined Functions](./udfs.md) - demonstrates how User Defined Functions (UDFs) are used to provide functionality within Splink that is not native to a given SQL backend.
* [Managing Splink's Dependencies](./dependency_compatibility_policy.md) - this section provides guidelines for managing our core dependencies and our strategy for phasing out Python versions that have reached their end-of-life.
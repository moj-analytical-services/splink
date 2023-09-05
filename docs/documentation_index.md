---
hide:
  - toc
---

# Documentation

This section contains reference material for the modules and functions within Splink.

## API
The documentation for the Splink API is broken up into the following categories:

- [Linker API](./linker.md) - for all of the methods that can be used with an instantiated `Linker` object.

- [Comparisons Library API](./comparison_level_library.md) - for methods that can be used to define `Comparisons` within a Splink Settings dictionary.

- [Blocking Rules Library API](./blocking_rule_library.md) - for methods that can be used to define a `Blocking Rule` for use within a Splink Settings dictionary or in the expectation maximisation step.

- [EM Training Session API](./em_training_session.md) - for methods that be used to inspect the results of individual iterations of the Expectation Maximisation model training algorithm.

- [Splink Dataframe API](./SplinkDataFrame.md) - for methods that can be used to manipulate a `SplinkDataFrame` across all SQL backends.

- [Comparisons API](./comparison.md) - for reference material giving more explanation on the `Comparison` and `ComparisonLevel` objects within Splink.

Note: When building a Splink model, the [Linker API](./linker.md) and [Comparisons Library API](./comparison_level_library.md) will be the most useful parts of the API documentation. The other sections are primarily for reference.

## Charts Gallery

The [Splink Charts Gallery](./charts/index.md) contains examples for all of the interactive charts and dashboards that are provided in Splink to help the linking process.

## In-built datasets
Information on pre-made data tables available within Splink suitable for linking, to get up-and-running or to try out ideas.

- [In-build datasets](./datasets.md) - information on included datasets, as well as how to use them, and methods for managing them.

## Splink Settings
Reference materials for the Splink Settings dictionary:

- [Settings Dictionary Reference](./settings_dict_guide.md) - for reference material on the parameters available within a Splink Settings dictionary.

- [Interactive Settings Editor](./settingseditor/editor.md) - for an interactive Settings editor, giving a hands-on sandbox to test out the Splink Settings dictionary. Including validation, autocomplete and autoformatting.
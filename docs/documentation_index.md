---
hide:
  - toc
---

# Documentation

The documentation of Splink can be broken into 2 main categories, API docs and Splink Settings docs. These are further broken down as explained below:

## API

- [Linker API](./linker.md) - for all of the methods that can be used with an instantiated `Linker` object.

- [Comparisons Library API](./comparison_level_library.md) - for methods that can be used to define `Comparisons` within a Splink Settings dictionary.

- [EM Training Session API](./em_training_session.md) - for methods that be used to inspect the results of individual iterations of the Expectation Maximisation model training algorithm.

- [Splink Dataframe API](./SplinkDataFrame.md) - for methods that can be used to manipulate a `SplinkDataFrame` across all SQL backends.

- [Comparisons API](./comparison.md) - for reference material giving more explanation on the `Comparison` and `ComparisonLevel` objects within Splink.

Note: When building a Splink model, the [Linker API](./linker.md) and [Comparisons Library API](./comparison_level_library.md) will be the most useful parts of the API documentation. The other sections are primarily for reference.

## Splink Settings

- [Settings Dictionary Reference](./settings_dict_guide.md) - for reference material on the parameters available within a Splink Settings dictionary.

- [Interactive Settings Editor](./settingseditor/editor.md) - for an interactive Settings editor, giving a hands-on sandbox to test out the Splink Settings dictionary. Including validation, autocomplete and autoformatting.
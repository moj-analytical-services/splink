---
tags:
  - API
  - comparisons
  - Levenshtein
  - Jaro-Winkler
  - Jaccard
  - Distance In KM
  - Date Difference
  - Array Intersect
toc_depth: 2
---
# Documentation for `comparison_library`

The `comparison_library` contains pre-made comparisons available for use directly [as described in this topic guide](./topic_guides/customising_comparisons.html#method-1-using-the-comparisonlibrary).
However, not every comparison is available for every [Splink-compatible SQL backend](./topic_guides/backends.html).

The pre-made Splink comparisons available for each SQL dialect are as given in this table:

{% include-markdown "./includes/generated_files/comparison_library_dialect_table.md" %}






The detailed API for each of these are outlined below.

## Library comparison APIs

::: splink.comparison_library.ExactMatchBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.DistanceFunctionAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.LevenshteinAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.DamerauLevenshteinAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.JaccardAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.JaroAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.JaroWinklerAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.ArrayIntersectAtSizesBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.DatediffAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_library.DistanceInKMAtThresholdsBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

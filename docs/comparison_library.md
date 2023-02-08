---
tags:
  - API
  - comparisons
  - Levenstein
  - Jaro-Winkler
  - Jaccard
---
# Documentation for `comparison_library` 

The `comparison_library` contains pre-made comparisons available for use directly [as described in this topic guide](../topic_guides/customising_comparisons.html#method-1-using-the-comparisonlibrary).
However, not every comparison is available for every [Splink-compatible SQL backend](../topic_guides/backends.html).

The pre-made Splink comparisons available for each SQL dialect are as given in this table:
{%
  include-markdown "./includes/comparison_library_dialect_table.md"
%}

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
      heading_level: 1

---

::: splink.comparison_library.DistanceFunctionAtThresholdsComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_library.LevenshteinAtThresholdsComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_library.JaccardAtThresholdsComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false   
      heading_level: 1

---

::: splink.comparison_library.JaroWinklerAtThresholdsComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_library.ArrayIntersectAtSizesComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_library.DateDiffAtThresholdsComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---
tags:
  - API
  - comparisons
  - Levenstein
  - Jaro-Winkler
  - Jaccard
---
# Documentation for `comparison_library` 

The pre-made comparisons available for each SQL dialect are detailed here:
{%
  include-markdown "./includes/comparison_library_detailed.md"
%}

The details of how to use each of these are outlined below.

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

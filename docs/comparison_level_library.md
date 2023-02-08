---
tags:
  - API
  - comparisons
  - Levenstein
  - Jaro-Winkler
  - Jaccard
---
# Documentation for `comparison_level_library` 

The pre-made comparison levels available for each SQL dialect are detailed here:
{%
  include-markdown "./includes/comparison_level_library_detailed.md"
%}

The details of how to use each of these are outlined below.

## Library comparison level APIs

::: splink.comparison_level_library.NullLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.ExactMatchLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1
      
---

::: splink.comparison_level_library.ElseLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.DistanceFunctionLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.LevenshteinLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false   
      heading_level: 1

---

::: splink.comparison_level_library.JaroWinklerLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.JaccardLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.ColumnsReversedLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

---

::: splink.comparison_level_library.DistanceInKMLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1    

---

::: splink.comparison_level_library.PercentageDifferenceLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1   

---

::: splink.comparison_level_library.ArrayIntersectLevelBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 1

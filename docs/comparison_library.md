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

||:simple-duckdb: <br> DuckDB|:simple-apachespark: <br> Spark|:simple-amazonaws: <br> Athena|:simple-sqlite: <br> SQLite|:simple-postgresql: <br> PostgreSql|
|:-:|:-:|:-:|:-:|:-:|:-:|
|[array_intersect_at_sizes](#splink.comparison_library.ArrayIntersectAtSizesBase)|✓|✓|✓||✓|
|[damerau_levenshtein_at_thresholds](#splink.comparison_library.DamerauLevenshteinAtThresholdsBase)|✓|✓||✓||
|[datediff_at_thresholds](#splink.comparison_library.DatediffAtThresholdsBase)|✓|✓|✓||✓|
|[distance_function_at_thresholds](#splink.comparison_library.DistanceFunctionAtThresholdsBase)|✓|✓|✓|✓|✓|
|[distance_in_km_at_thresholds](#splink.comparison_library.DistanceInKmAtThresholdsBase)|✓|✓|✓||✓|
|[exact_match](#splink.comparison_library.ExactMatchBase)|✓|✓|✓|✓|✓|
|[jaccard_at_thresholds](#splink.comparison_library.JaccardAtThresholdsBase)|✓|✓||||
|[jaro_at_thresholds](#splink.comparison_library.JaroAtThresholdsBase)|✓|✓||✓||
|[jaro_winkler_at_thresholds](#splink.comparison_library.JaroWinklerAtThresholdsBase)|✓|✓||✓||
|[levenshtein_at_thresholds](#splink.comparison_library.LevenshteinAtThresholdsBase)|✓|✓|✓|✓|✓|






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

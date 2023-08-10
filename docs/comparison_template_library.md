---
tags:
  - API
  - comparisons
  - Date Comparison
toc_depth: 2
---

# Documentation for `comparison_template_library`

The `comparison_template_library` contains pre-made comparisons with pre-defined parameters available for use directly [as described in this topic guide](./topic_guides/customising_comparisons.html#method-2-using-the-comparisontemplatelibrary).
However, not every comparison is available for every [Splink-compatible SQL backend](./topic_guides/backends.html). More detail on creating comparisons for specific data types is also [included in the topic guide.](./topic_guides/customising_comparisons.html#creating-comparisons-for-specific-data-types)

The pre-made Splink comparison templates available for each SQL dialect are as given in this table:

||:simple-duckdb: <br> DuckDB|:simple-apachespark: <br> Spark|:simple-amazonaws: <br> Athena|:simple-sqlite: <br> SQLite|:simple-postgresql: <br> PostgreSql|
|:-:|:-:|:-:|:-:|:-:|:-:|
|[date_comparison](#splink.comparison_template_library.DateComparisonBase)|✓|✓||||
|[email_comparison](#splink.comparison_template_library.EmailComparisonBase)|✓|✓||||
|[forename_surname_comparison](#splink.comparison_template_library.ForenameSurnameComparisonBase)|✓|✓||✓||
|[name_comparison](#splink.comparison_template_library.NameComparisonBase)|✓|✓||✓||
|[postcode_comparison](#splink.comparison_template_library.PostcodeComparisonBase)|✓|✓|✓|||




The detailed API for each of these are outlined below.

## Library comparison APIs

::: splink.comparison_template_library.DateComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_template_library.NameComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_template_library.ForenameSurnameComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_template_library.PostcodeComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---

::: splink.comparison_template_library.EmailComparisonBase
    handler: python
    selection:
      members:
        -  __init__
    rendering:
      show_root_heading: true
      show_source: false
      heading_level: 3

---
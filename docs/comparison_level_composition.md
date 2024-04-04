---
tags:
  - API
  - comparisons
---
# Documentation for `comparison_level_composition` functions

`comparison_composition` allows the merging of existing comparison levels by a logical SQL clause - `OR`, `AND` or `NOT`.

This extends the functionality of our base comparison levels by allowing users to "join" existing comparisons by various SQL clauses.

For example, `or_(null_level("first_name"), null_level("surname"))` creates a check for nulls in *either* `first_name` or `surname`, rather than restricting the user to a single column.

The Splink comparison level composition functions available for each SQL dialect are as given in this table:

{% include-markdown "./includes/generated_files/comparison_composition_library_dialect_table.md" %}



The detailed API for each of these are outlined below.

## Library comparison composition APIs

::: splink.comparison_level_composition
    handler: python
    selection:
      members:
        - and_
        - or_
        - not_

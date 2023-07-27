---
tags:
  - API
  - blocking rule
---
# Documentation for `blocking_rule_composition` functions

`blocking_composition` allows the merging of existing blocking rules by a logical SQL clause - `OR`, `AND` or `NOT`.

This extends the functionality of our base blocking rules by allowing users to "join" existing comparisons by various SQL clauses.

For example, `or_(exact_match_rule("first_name"), exact_match_rule("surname"))` creates a dual check for an exact match in *either* `first_name` or `surname`, rather than restricting the user to a single blocking rule.

The detailed API for each of these are outlined below.

## Library comparison composition APIs

::: splink.blocking_rule_composition
    handler: python
    selection:
      members:
        - and_
        - or_
        - not_

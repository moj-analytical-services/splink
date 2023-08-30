---
tags:
  - API
  - blocking rule
---
# Documentation for `blocking_rule_composition` functions

`blocking_composition` allows the merging of existing blocking rules by a logical SQL clause - `AND`, `OR` or `NOT`.

This extends the functionality of our base blocking rules by allowing users to "join" existing comparisons by various SQL clauses.

For example, `and_(block_on("first_name"), block_on("surname"))` creates a dual check for an exact match where both `first_name` and `surname` are equal.

The detailed API for each of these are outlined below.

## Library comparison composition APIs

::: splink.blocking_rule_composition
    handler: python
    selection:
      members:
        - and_
        - or_
        - not_

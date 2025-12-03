# Migration Guide: Splink v4 to v5

This guide explains the breaking changes in Splink v5 and how to update your code.

## Summary of Changes

Splink v5 introduces a new data registration API that simplifies the workflow and eliminates redundant data registration. The key changes are:

1. **Data registration is now explicit** - Use `db_api.register()` to register data once upfront
2. **`db_api` parameter removed from most functions** - Functions now infer `db_api` from the `SplinkDataFrame` input
3. **`input_table_aliases` replaced by `alias` parameter** - Table aliases are now set at registration time

## New API Pattern

### Before (v4)

```python
from splink import DuckDBAPI, Linker, block_on
from splink.blocking_analysis import count_comparisons_from_blocking_rule
from splink.exploratory import profile_columns, completeness_chart

db_api = DuckDBAPI()
df = pd.read_csv("my_data.csv")

# db_api passed to every function
count_comparisons_from_blocking_rule(
    table_or_tables=df,
    blocking_rule=block_on("first_name"),
    link_type="dedupe_only",
    db_api=db_api,  # Required in v4
)

profile_columns(df, db_api=db_api)  # Required in v4

completeness_chart(df, db_api=db_api)  # Required in v4

linker = Linker(df, settings, db_api=db_api)  # Required in v4
```

### After (v5)

```python
from splink import DuckDBAPI, Linker, block_on
from splink.blocking_analysis import count_comparisons_from_blocking_rule
from splink.exploratory import profile_columns, completeness_chart

db_api = DuckDBAPI()
df = pd.read_csv("my_data.csv")

# Register data once - reuse everywhere
sdf = db_api.register(df)

# No db_api parameter needed
count_comparisons_from_blocking_rule(
    table_or_tables=sdf,
    blocking_rule=block_on("first_name"),
    link_type="dedupe_only",
)

profile_columns(sdf)

completeness_chart(sdf)

linker = Linker(sdf, settings)  # db_api inferred from sdf
```

## Multi-table Linking

### Before (v4)

```python
linker = Linker(
    [df_left, df_right],
    settings,
    db_api=db_api,
    input_table_aliases=["customers", "contacts"],
)
```

### After (v5)

```python
# Aliases specified at registration time
sdf_left = db_api.register(df_left, alias="customers")
sdf_right = db_api.register(df_right, alias="contacts")

linker = Linker([sdf_left, sdf_right], settings)
```

## Registering Multiple Tables

For convenience, you can register multiple tables at once:

```python
# Register multiple tables
sdfs = db_api.register_multiple([df_left, df_right])

# Or with aliases
sdfs = db_api.register_multiple(
    [df_left, df_right],
    aliases=["customers", "contacts"]
)
```

## Backward Compatibility

The `Linker` class still supports the legacy API for backward compatibility:

```python
# This still works in v5
linker = Linker(df, settings, db_api=db_api)
```

However, the blocking analysis, profiling, and completeness functions **require** `SplinkDataFrame` inputs and no longer accept a `db_api` parameter.

## Benefits of the New API

1. **Less boilerplate** - `db_api` is used once at registration time
2. **Avoid redundant registration** - Data is registered once in the database, not re-registered by each function
3. **Cleaner function signatures** - Functions receive ready-to-use `SplinkDataFrame` objects
4. **Better separation of concerns** - Registration logic lives in `db_api`, business logic in functions

## Quick Reference

| v4 Function | v5 Change |
|-------------|-----------|
| `count_comparisons_from_blocking_rule(..., db_api=db_api)` | `count_comparisons_from_blocking_rule(sdf, ...)` |
| `cumulative_comparisons_to_be_scored_from_blocking_rules_chart(..., db_api=db_api)` | `cumulative_comparisons_to_be_scored_from_blocking_rules_chart(sdf, ...)` |
| `n_largest_blocks(..., db_api=db_api)` | `n_largest_blocks(sdf, ...)` |
| `profile_columns(df, db_api=db_api)` | `profile_columns(sdf)` |
| `completeness_chart(df, db_api=db_api)` | `completeness_chart(sdf)` |
| `Linker(df, settings, db_api=db_api)` | `Linker(sdf, settings)` |
| `Linker([df1, df2], settings, db_api, input_table_aliases=["a","b"])` | `Linker([sdf1, sdf2], settings)` with aliases at registration |

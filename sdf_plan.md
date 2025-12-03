# SplinkDataFrame Registration API Refactor

## Rationale

### Current Problem

The current Splink API requires passing `db_api` to every function:

```python
db_api = DuckDBAPI()
df = pd.read_csv("path")

# db_api passed here...
count_comparisons_from_blocking_rule(
    table_or_tables=df,
    blocking_rule=block_on("first_name"),
    link_type="dedupe_only",
    db_api=db_api,
)

# ...and here...
cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=df,
    blocking_rules=[block_on("first_name")],
    link_type="dedupe_only",
    db_api=db_api,
)

# ...and here
linker = Linker(df, settings, db_api)
```

This has several issues:

1. **Repetitive boilerplate** - `db_api` is passed everywhere
2. **Hidden re-registration** - Each function internally calls `db_api.register_multiple_tables()`, so the same pandas DataFrame gets registered multiple times with different table names
3. **Mixed responsibilities** - Functions mix "data preparation" with "business logic"
4. **Signature bloat** - Every function needs a `db_api` parameter

### Proposed Solution

Users explicitly register data once upfront:

```python
db_api = DuckDBAPI()
df = pd.read_csv("path")
sdf = db_api.register(df)  # Register once, reuse everywhere

count_comparisons_from_blocking_rule(
    table_or_tables=sdf,
    blocking_rule=block_on("first_name"),
    link_type="dedupe_only",
)

cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    table_or_tables=sdf,
    blocking_rules=[block_on("first_name")],
    link_type="dedupe_only",
)

linker = Linker(sdf, settings)  # db_api inferred from sdf
```

## Aims

1. **Reduce boilerplate** - `db_api` is used once at registration time
2. **Avoid redundant registration** - Data is registered once in the database
3. **Cleaner function signatures** - Functions receive ready-to-use `SplinkDataFrame` objects
4. **Better separation of concerns** - Registration logic lives in `db_api`, business logic in functions
5. **Easier testing** - Functions can receive mock `SplinkDataFrame` objects
6. **Simpler codebase** - Remove `db_api` parameters entirely from function signatures (breaking change)

**Note:** This is a breaking change for a major version release. We intentionally do NOT maintain backward compatibility, which allows us to fully simplify the internal codebase by removing the `db_api` parameter from all function signatures.

## Implementation Plan

### Phase 1: Add `db_api.register()` Method

#### Step 1.1: Add `register()` method to base `DatabaseAPI`

**File:** `splink/internals/database_api.py`

Add a new public method `register()` that wraps the existing `register_table()` functionality but returns a `SplinkDataFrame` directly.

```python
def register(
    self,
    table: AcceptableInputTableType,
    table_name: Optional[str] = None,
    alias: Optional[str] = None,
) -> SplinkDataFrame:
    """Register a table with this database backend.

    Args:
        table: Input data (pandas DataFrame, pyarrow Table, etc.)
        table_name: Optional name for the table in the database. Auto-generated if not provided.
        alias: Optional human-readable alias for the table, used in Splink outputs
               (e.g. "customers", "contact_center_callers"). If not provided,
               defaults to the table_name.

    Returns:
        A SplinkDataFrame bound to this database backend.
    """
```

The `alias` parameter replaces the current `input_table_aliases` parameter on `Linker.__init__()`. This is cleaner because:
- The alias is bound to the data at registration time, not at Linker creation
- Each table's alias is specified alongside its registration (not in a separate parallel list)
- The `SplinkDataFrame` carries its alias, so the Linker can just read it

**Verification:**
```python
db_api = DuckDBAPI()
df = pd.DataFrame({"a": [1, 2, 3]})
sdf = db_api.register(df, alias="customers")
assert isinstance(sdf, SplinkDataFrame)
assert sdf.db_api is db_api
assert sdf.alias == "customers"  # or similar property
print(sdf.as_pandas_dataframe())  # Should print the data
```

#### Step 1.2: Add `register_multiple()` method to base `DatabaseAPI`

**File:** `splink/internals/database_api.py`

Add method for registering multiple tables (for link_only scenarios):

```python
def register_multiple(
    self,
    tables: dict[str, AcceptableInputTableType] | list[AcceptableInputTableType],
    table_names: Optional[list[str]] = None,
    aliases: Optional[list[str]] = None,
) -> list[SplinkDataFrame]:
    """Register multiple tables with this database backend.

    Args:
        tables: Input tables as a dict (keys become aliases) or list
        table_names: Optional names for tables in the database
        aliases: Optional human-readable aliases for each table (used in Splink outputs)

    Returns:
        List of SplinkDataFrame objects
    """
```

**Verification:**
```python
db_api = DuckDBAPI()
df_a = pd.DataFrame({"id": [1, 2]})
df_b = pd.DataFrame({"id": [3, 4]})

# Using list with explicit aliases
sdfs = db_api.register_multiple([df_a, df_b], aliases=["customers", "leads"])
assert len(sdfs) == 2
assert sdfs[0].alias == "customers"
assert sdfs[1].alias == "leads"

# Or using dict where keys become aliases
sdfs = db_api.register_multiple({"customers": df_a, "leads": df_b})
assert sdfs[0].alias == "customers"
```

---

### Phase 2: Update Standalone Functions to Accept `SplinkDataFrame`

#### Step 2.1: Create helper to extract `db_api` from `SplinkDataFrame` inputs

**File:** `splink/internals/input_column_handling.py` (or new file)

Create a utility function that:
- Extracts `db_api` from `SplinkDataFrame` input(s)
- Validates all inputs are `SplinkDataFrame` (raises error if raw data passed)
- Validates all inputs share the same `db_api` (for multiple tables)

```python
def get_db_api_from_inputs(
    table_or_tables: SplinkDataFrame | list[SplinkDataFrame],
) -> DatabaseAPI:
    """Extract db_api from SplinkDataFrame input(s).

    Raises:
        TypeError: If input is not a SplinkDataFrame
        ValueError: If multiple inputs have different db_api instances
    """
```

**Verification:** Unit test the helper with various input combinations.

#### Step 2.2: Update `count_comparisons_from_blocking_rule()`

**File:** `splink/internals/blocking_analysis.py`

- **Remove** `db_api` parameter entirely from function signature
- Use helper from 2.1 to extract `db_api` from the `SplinkDataFrame` input
- Update type hints to require `SplinkDataFrame` instead of `AcceptableInputTableType`

**Verification:**
```python
db_api = DuckDBAPI()
df = splink_datasets.fake_1000
sdf = db_api.register(df)

# New API - only way to call the function
result = count_comparisons_from_blocking_rule(
    table_or_tables=sdf,
    blocking_rule=block_on("first_name"),
    link_type="dedupe_only",
)

# Old API no longer works - raises TypeError
# count_comparisons_from_blocking_rule(df, ..., db_api=db_api)  # ERROR
```

#### Step 2.3: Update `cumulative_comparisons_to_be_scored_from_blocking_rules_chart()`

**File:** `splink/internals/blocking_analysis.py`

Same changes as 2.2.

**Verification:** Similar test pattern.

#### Step 2.4: Update remaining standalone blocking analysis functions

**Files:** `splink/internals/blocking_analysis.py`

Apply same pattern to:
- `n_largest_blocks()`
- `cumulative_comparisons_to_be_scored_from_blocking_rules_data()`

**Verification:** Test each function with new API.

---

### Phase 3: Update `Linker` to Accept `SplinkDataFrame`

#### Step 3.1: Update `Linker.__init__()` to require `SplinkDataFrame`

**File:** `splink/internals/linker.py`

- **Remove** `db_api` parameter entirely from `Linker.__init__()` signature
- **Remove** `input_table_aliases` parameter (aliases are now on the `SplinkDataFrame` objects)
- Require input to be `SplinkDataFrame` (or list of `SplinkDataFrame`)
- Extract `db_api` from the input `SplinkDataFrame`
- Read aliases from the input `SplinkDataFrame` objects
- Update type hints accordingly

**New signature:**
```python
def __init__(
    self,
    input_table_or_tables: SplinkDataFrame | list[SplinkDataFrame],
    settings: SettingsCreator | dict[str, Any] | Path | str,
    set_up_basic_logging: bool = True,
    validate_settings: bool = True,
):
```

**Verification:**
```python
db_api = DuckDBAPI()
df = splink_datasets.fake_1000
sdf = db_api.register(df, alias="my_data")

# New API - only way to create Linker
linker = Linker(sdf, settings)
assert linker.db_api is db_api

# Link with aliases specified at registration
df_1 = pd.read_parquet("table_1/")
df_2 = pd.read_parquet("table_2/")
sdf_1 = db_api.register(df_1, alias="customers")
sdf_2 = db_api.register(df_2, alias="contact_center_callers")
linker = Linker([sdf_1, sdf_2], settings)

# Old API no longer works
# Linker(df, settings, db_api)  # ERROR
# Linker([df_1, df_2], settings, db_api, input_table_aliases=[...])  # ERROR
```

#### Step 3.2: Validate consistent `db_api` for multiple tables

**File:** `splink/internals/linker.py`

When multiple `SplinkDataFrame` objects are passed, validate they all share the same `db_api`.

**Verification:**
```python
db_api1 = DuckDBAPI()
db_api2 = DuckDBAPI()
sdf1 = db_api1.register(df1)
sdf2 = db_api2.register(df2)

# Should raise clear error
linker = Linker([sdf1, sdf2], settings)  # Error: mismatched db_api
```

---

### Phase 4: Update Profile Functions

#### Step 4.1: Update `profile_columns()`

**File:** `splink/internals/profile_data.py` (or wherever it lives)

Same pattern as blocking analysis functions.

**Verification:** Test with `SplinkDataFrame` input.

#### Step 4.2: Update `completeness_chart()` and related profiling functions

Apply same pattern to all remaining profiling functions.

---

### Phase 5: Documentation and Migration Guide

#### Step 5.1: Update documentation

- Update getting started guide to use new API
- Update API reference
- Add migration guide explaining the breaking changes

#### Step 5.2: Update example notebooks

Update all example notebooks to use new API pattern.

#### Step 5.3: Update CHANGELOG

Document the breaking changes clearly for users upgrading from previous versions.

---

## Testing Strategy

Each step should include:

1. **Unit tests** for the specific functionality added
2. **Integration test** showing end-to-end workflow with new API
3. **Error handling tests** ensuring helpful errors when raw data is passed without registration

## Rollout Strategy

This is a **breaking change** for a major version release (e.g., 5.0.0):

1. Complete all phases in a development branch
2. Update all tests to use new API
3. Update all documentation and examples
4. Release as major version with clear migration guide

# Plan to Remove Salting from Splink

## Overview

This plan removes salting functionality from Splink in preparation for Splink 5's chunking approach. The goal is to eliminate all salting-related code while preserving any functionality that will be reused for chunking.

**Key insight**: Chunking will use similar concepts (partitioning the comparison space) but via a different mechanism - hash-based chunk assignment on the composite unique ID rather than random salt values on join conditions.

## What to Preserve for Chunking

The following will be **kept** as they're needed for chunking:
- `_composite_unique_id_from_nodes_sql()` in [unique_id_concat.py](splink/internals/unique_id_concat.py) - will hash this for chunk assignment
- `vertically_concatenate_sql()` function structure - chunking may need modifications here
- The general `BlockingRule.create_blocked_pairs_sql()` pattern

---

## Step-by-Step Implementation Plan

### Phase 1: Remove `salting_partitions` from Public API

#### Step 1.1: Remove `salting_partitions` from `block_on()` function
**File**: [splink/internals/blocking_rule_library.py#L201-L244](splink/internals/blocking_rule_library.py#L201-L244)

**Changes**:
- Remove `salting_partitions` parameter from `block_on()` function signature
- Remove docstring reference to salting
- Remove the `if salting_partitions:` block that sets `br._salting_partitions`

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_library.py
uv run pytest tests/test_blocking.py -v
```

---

#### Step 1.2: Remove `salting_partitions` from `BlockingRuleCreator` base class
**File**: [splink/internals/blocking_rule_creator.py](splink/internals/blocking_rule_creator.py)

**Changes**:
- Remove `salting_partitions` parameter from `__init__`
- Remove `self._salting_partitions` assignment
- Remove `salting_partitions` property
- Remove `"salting_partitions"` from `create_blocking_rule_dict()`
- Remove the validation `if self.salting_partitions and self.arrays_to_explode`

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_creator.py
```

---

#### Step 1.3: Remove `salting_partitions` from `ExactMatchRule`
**File**: [splink/internals/blocking_rule_library.py#L23-L37](splink/internals/blocking_rule_library.py#L23-L37)

**Changes**:
- Remove `salting_partitions` parameter from `ExactMatchRule.__init__`
- Remove it from the `super().__init__()` call

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_library.py
```

---

#### Step 1.4: Remove `salting_partitions` from `CustomRule`
**File**: [splink/internals/blocking_rule_library.py#L40-L107](splink/internals/blocking_rule_library.py#L40-L107)

**Changes**:
- Remove `salting_partitions` parameter from `CustomRule.__init__`
- Remove from docstring
- Remove from `super().__init__()` call
- Remove the example with `salting_partitions=10`

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_library.py
```

---

#### Step 1.5: Remove `salting_partitions` from `_Merge` class
**File**: [splink/internals/blocking_rule_library.py#L110-L166](splink/internals/blocking_rule_library.py#L110-L166)

**Changes**:
- Remove `salting_partitions` parameter from `_Merge.__init__`
- Remove from `super().__init__()` call
- Remove the entire `salting_partitions` property (which computes max of child rules)

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_library.py
```

---

#### Step 1.6: Remove `salting_partitions` from `Not` class
**File**: [splink/internals/blocking_rule_library.py#L175-L191](splink/internals/blocking_rule_library.py#L175-L191)

**Changes**:
- Remove the `salting_partitions` property that delegates to child

**Verification**:
```bash
uv run mypy splink/internals/blocking_rule_library.py
```

---

### Phase 2: Remove `SaltedBlockingRule` Class

#### Step 2.1: Remove `SaltedBlockingRule` class definition
**File**: [splink/internals/blocking.py#L292-L356](splink/internals/blocking.py#L292-L356)

**Changes**:
- Delete the entire `SaltedBlockingRule` class

**Verification**:
```bash
uv run mypy splink/internals/blocking.py
```

---

#### Step 2.2: Remove `salting_partitions` from `BlockingRuleDict` TypedDict
**File**: [splink/internals/blocking.py#L34-L38](splink/internals/blocking.py#L34-L38)

**Changes**:
- Remove `salting_partitions: int | None` from the TypedDict

**Verification**:
```bash
uv run mypy splink/internals/blocking.py
```

---

#### Step 2.3: Update `blocking_rule_to_obj()` function
**File**: [splink/internals/blocking.py#L41-L72](splink/internals/blocking.py#L41-L72)

**Changes**:
- Remove the `salting_partitions = br.get("salting_partitions", None)` line
- Remove the validation `if arrays_to_explode is not None and salting_partitions is not None`
- Remove the `if salting_partitions is not None:` block that creates `SaltedBlockingRule`

**Verification**:
```bash
uv run mypy splink/internals/blocking.py
```

---

### Phase 3: Remove `__splink_salt` Column Creation

#### Step 3.1: Remove `salting_required` parameter from `vertically_concatenate_sql()`
**File**: [splink/internals/vertically_concatenate.py#L18-L77](splink/internals/vertically_concatenate.py#L18-L77)

**Changes**:
- Remove `salting_required: bool` parameter
- Remove the `if salting_required:` block that adds `salt_sql`
- Remove all references to `salt_sql` in the SQL generation

**Verification**:
```bash
uv run mypy splink/internals/vertically_concatenate.py
```

---

#### Step 3.2: Update all callers of `vertically_concatenate_sql()`
**Files to update**:
- [splink/internals/vertically_concatenate.py#L89-L200](splink/internals/vertically_concatenate.py#L89-L200) - `enqueue_df_concat_with_tf`, `compute_df_concat_with_tf`, `enqueue_df_concat`, `compute_df_concat`, `concat_table_column_names`
- [splink/internals/blocking.py#L514](splink/internals/blocking.py#L514) - `materialise_exploded_id_tables`
- [splink/internals/blocking_analysis.py](splink/internals/blocking_analysis.py) - multiple functions
- [splink/internals/profile_data.py#L253](splink/internals/profile_data.py#L253)
- [splink/internals/completeness.py#L27](splink/internals/completeness.py#L27)

**Changes**:
- Remove `salting_required=...` argument from all calls

**Verification**:
```bash
uv run mypy splink/internals/vertically_concatenate.py
uv run mypy splink/internals/blocking.py
uv run mypy splink/internals/blocking_analysis.py
uv run mypy splink/internals/profile_data.py
uv run mypy splink/internals/completeness.py
```

---

#### Step 3.3: Remove `salting_required` property from `Settings` class
**File**: [splink/internals/settings.py#L665-L675](splink/internals/settings.py#L665-L675)

**Changes**:
- Delete the entire `salting_required` property
- Remove the import of `SaltedBlockingRule` at the top of the file

**Verification**:
```bash
uv run mypy splink/internals/settings.py
```

---

#### Step 3.4: Remove `__splink_salt` filtering from clustering
**File**: [splink/internals/linker_components/clustering.py#L157-L158](splink/internals/linker_components/clustering.py#L157-L158) and [L324-L325](splink/internals/linker_components/clustering.py#L324-L325)

**Changes**:
- Remove the `columns_without_salt = filter(lambda x: x != "__splink_salt", columns)` lines
- Simplify to use `columns` directly

**Verification**:
```bash
uv run mypy splink/internals/linker_components/clustering.py
```

---

#### Step 3.5: Remove `__splink_salt` from `concat_table_column_names()`
**File**: [splink/internals/vertically_concatenate.py#L186-L211](splink/internals/vertically_concatenate.py#L186-L211)

**Changes**:
- Remove `salting_required = linker._settings_obj.salting_required`
- Remove `if salting_required: columns.append("__splink_salt")`

**Verification**:
```bash
uv run mypy splink/internals/vertically_concatenate.py
```

---

### Phase 4: Remove Salting from Training/Estimation

#### Step 4.1: Remove salted blocking rule from `estimate_u_values()`
**File**: [splink/internals/estimate_u.py#L150-L159](splink/internals/estimate_u.py#L150-L159)

**Changes**:
- Remove the DuckDB-specific salting logic block that creates a `BlockingRuleDict` with `salting_partitions: multiprocessing.cpu_count()`
- Keep the cartesian product logic (`blocking_rule: "1=1"`) but without salting

**Verification**:
```bash
uv run mypy splink/internals/estimate_u.py
```

---

#### Step 4.2: Remove `SaltedBlockingRule` import from training.py
**File**: [splink/internals/linker_components/training.py#L8](splink/internals/linker_components/training.py#L8)

**Changes**:
- Remove `SaltedBlockingRule` from the import statement
- Update line 293 that checks `isinstance(blocking_rule_obj, (BlockingRule, SaltedBlockingRule))` to just check `BlockingRule`

**Verification**:
```bash
uv run mypy splink/internals/linker_components/training.py
```

---

### Phase 5: Update Tests

#### Step 5.1: Remove salting tests
**File**: [tests/test_salting_len.py](tests/test_salting_len.py)

**Changes**:
- Delete the entire file (it only tests salting functionality)

**Verification**:
```bash
uv run pytest -m spark --collect-only 2>&1 | grep -v test_salting
```

---

#### Step 5.2: Update `test_blocking.py`
**File**: [tests/test_blocking.py](tests/test_blocking.py)

**Changes**:
- Remove tests that use `salting_partitions` parameter
- Update test assertions that check for `.salting_partitions`

**Verification**:
```bash
uv run pytest tests/test_blocking.py -v
```

---

#### Step 5.3: Update `test_full_example_spark.py`
**File**: [tests/test_full_example_spark.py#L55](tests/test_full_example_spark.py#L55)

**Changes**:
- Remove `"salting_partitions": 3` from the blocking rule dict

**Verification**:
```bash
uv run pytest tests/test_full_example_spark.py -v -m spark
```

---

#### Step 5.4: Update `test_blocking_rule_composition.py`
**File**: [tests/test_blocking_rule_composition.py](tests/test_blocking_rule_composition.py)

**Changes**:
- Remove tests that check salting partition propagation through composed rules

**Verification**:
```bash
uv run pytest tests/test_blocking_rule_composition.py -v
```

---

### Phase 6: Final Cleanup

#### Step 6.1: Remove multiprocessing import from estimate_u.py
**File**: [splink/internals/estimate_u.py#L4](splink/internals/estimate_u.py#L4)

**Changes**:
- Remove `import multiprocessing` (only used for salting)

**Verification**:
```bash
uv run mypy splink/internals/estimate_u.py
```

---

#### Step 6.2: Run full test suite
**Verification**:
```bash
uv run pytest -m duckdb
uv run pytest -m spark  # if Spark is available
```

---

## Summary Table

| Step | File(s) | What's Removed |
|------|---------|----------------|
| 1.1 | blocking_rule_library.py | `salting_partitions` from `block_on()` |
| 1.2 | blocking_rule_creator.py | `salting_partitions` from base class |
| 1.3 | blocking_rule_library.py | `salting_partitions` from `ExactMatchRule` |
| 1.4 | blocking_rule_library.py | `salting_partitions` from `CustomRule` |
| 1.5 | blocking_rule_library.py | `salting_partitions` from `_Merge` |
| 1.6 | blocking_rule_library.py | `salting_partitions` from `Not` |
| 2.1 | blocking.py | `SaltedBlockingRule` class |
| 2.2 | blocking.py | `salting_partitions` from TypedDict |
| 2.3 | blocking.py | Salting logic in `blocking_rule_to_obj()` |
| 3.1 | vertically_concatenate.py | `salting_required` parameter |
| 3.2 | Multiple files | All `salting_required=...` call sites |
| 3.3 | settings.py | `salting_required` property |
| 3.4 | clustering.py | `__splink_salt` filtering |
| 3.5 | vertically_concatenate.py | Salt column in `concat_table_column_names()` |
| 4.1 | estimate_u.py | Salted blocking for DuckDB u-estimation |
| 4.2 | training.py | `SaltedBlockingRule` import/usage |
| 5.1-5.4 | tests/ | Salting-related tests |
| 6.1 | estimate_u.py | `multiprocessing` import |

---

## Risk Assessment

**Low Risk**:
- Removing the `salting_partitions` parameter from public API is a breaking change, but this is for Splink 5
- All changes are self-contained and can be verified incrementally

**Medium Risk**:
- DuckDB parallelisation currently relies on salting; without it, performance on multi-core machines may regress until chunking is implemented
  - **Mitigation**: Document that chunking should be implemented shortly after this removal

**What's Preserved for Chunking**:
- The `_composite_unique_id_from_nodes_sql()` function is untouched and ready for hash-based chunk assignment
- The general structure of `create_blocked_pairs_sql()` pattern remains
- The vertical concatenation infrastructure is preserved, just simplified

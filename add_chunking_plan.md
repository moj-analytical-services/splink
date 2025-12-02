# Plan to Add Chunking to Splink 5

## Overview

This plan implements chunking functionality for `predict()` to enable:
- **Progress feedback**: Know how far through a linkage you are
- **Memory control**: Run large linkages sequentially on small machines
- **Multi-machine distribution**: Assign chunks to different machines
- **Time estimation**: Run 1 chunk to estimate total time

---

## API Design: Two-Function Approach

### User-Facing API

**`predict()`** - The main function. When chunking is requested, it runs all chunks internally and combines results:
```python
# Standard prediction (no chunking)
df_predict = linker.inference.predict()

# Automatic chunking - runs 30 chunks internally, combines results
# User gets back exactly what they'd get without chunking
df_predict = linker.inference.predict(left_chunks=3, right_chunks=10)
```

**`predict_chunk()`** - Low-level function for advanced users (multi-machine distribution):
```python
# Run a single chunk manually (1/30th of the data)
df_chunk = linker.inference.predict_chunk(
    left_chunk=(1, 3),   # chunk 1 of 3 on left
    right_chunk=(3, 10)  # chunk 3 of 10 on right
)
```

### Why Two Functions?

| Use Case | Function | Benefit |
|----------|----------|---------|
| Simple chunked prediction | `predict(left_chunks=3, right_chunks=10)` | Just works, automatic combination |
| Multi-machine distribution | `predict_chunk(left_chunk=(1,3), right_chunk=(3,10))` | Run different chunks on different machines |
| Time estimation | `predict_chunk(left_chunk=(1,10), right_chunk=(1,10))` | Run 1% to estimate full time |
| Memory-constrained | `predict(left_chunks=5, right_chunks=5)` | Process 1/25th at a time sequentially |

### Key Testing Property

This design enables a powerful correctness test:
```python
# These MUST produce identical results
df_no_chunk = linker.inference.predict()
df_chunked = linker.inference.predict(left_chunks=3, right_chunks=4)

assert df_no_chunk equals df_chunked  # Same pairs, same scores
```

---

## Key Mechanism

Hash-based deterministic partitioning using the composite unique ID:
```sql
-- Chunk assignment via hash of composite unique ID + modulo
(ABS(hash(source_dataset || '-__-' || unique_id)) % num_chunks) + 1
```

---

## Prerequisites

- Salting has been removed (per `remove_salting.md`)
- The `_composite_unique_id_from_nodes_sql()` helper exists in [unique_id_concat.py](splink/internals/unique_id_concat.py)

---

## Step-by-Step Implementation Plan

### Phase 1: Add Hash Function to Dialect Classes

Each SQL dialect may have different hash function syntax. Add a method to generate consistent integer hashes.

#### Step 1.1: Add `hash_function_name` property to `SplinkDialect` base class
**File**: [splink/internals/dialects.py](splink/internals/dialects.py)

**Changes**:
- Add abstract property `hash_function_name` to `SplinkDialect` base class
- Default implementation raises NotImplementedError

```python
@property
def hash_function_name(self) -> str:
    """Return the name of a hash function that returns a bigint/int64"""
    raise NotImplementedError(
        f"hash_function_name not implemented for {self.__class__.__name__}"
    )
```

**Verification**:
```bash
uv run mypy splink/internals/dialects.py
```

---

#### Step 1.2: Implement `hash_function_name` for DuckDB dialect
**File**: [splink/internals/dialects.py](splink/internals/dialects.py#L225)

**Changes**:
- Add to `DuckDBDialect` class:
```python
@property
def hash_function_name(self) -> str:
    return "hash"  # DuckDB's hash() returns int64
```

**Verification**:
```bash
uv run python -c "import duckdb; print(duckdb.sql('select hash(\"test\")').fetchall())"
```

---

#### Step 1.3: Implement `hash_function_name` for Spark dialect
**File**: [splink/internals/dialects.py](splink/internals/dialects.py#L360)

**Changes**:
- Add to `SparkDialect` class:
```python
@property
def hash_function_name(self) -> str:
    return "hash"  # Spark's hash() returns int
```

**Verification**: Manual test in Spark environment or skip for now (DuckDB is priority).

---

#### Step 1.4: Implement `hash_function_name` for other dialects
**Files**: [splink/internals/dialects.py](splink/internals/dialects.py)

**Changes**:
- SQLite: Use `abs(random())` workaround or custom UDF (document limitation)
- Postgres: Use `hashtext()` wrapped to return bigint
- Athena: Use `xxhash64()`

**Verification**:
```bash
uv run mypy splink/internals/dialects.py
```

---

### Phase 2: Create Chunking SQL Helper Functions

#### Step 2.1: Create new file `splink/internals/chunking.py`
**File**: `splink/internals/chunking.py` (NEW)

**Content**:
```python
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from splink.internals.input_column import InputColumn
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql

if TYPE_CHECKING:
    from splink.internals.dialects import SplinkDialect


def _chunk_assignment_sql(
    unique_id_cols: list[InputColumn],
    num_chunks: int,
    table_prefix: str,
    dialect: "SplinkDialect",
) -> str:
    """
    Generate SQL expression that assigns a row to a chunk number (1 to num_chunks).

    Uses deterministic hashing of the composite unique ID.

    Args:
        unique_id_cols: The columns that form the unique ID
        num_chunks: Total number of chunks to partition into
        table_prefix: Table alias prefix (e.g., 'l' or 'r')
        dialect: SQL dialect for hash function

    Returns:
        SQL expression like: "(ABS(hash(...)) % 3) + 1"
    """
    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix)
    hash_func = dialect.hash_function_name
    return f"(ABS({hash_func}({composite_id})) % {num_chunks}) + 1"


def validate_chunk_param(
    chunk: Optional[Tuple[int, int]], param_name: str
) -> Tuple[int, int] | None:
    """
    Validate and normalize a chunk parameter.

    Args:
        chunk: Tuple of (chunk_number, total_chunks) or None
        param_name: Name of parameter for error messages

    Returns:
        Validated tuple or None

    Raises:
        ValueError: If chunk parameters are invalid
    """
    if chunk is None:
        return None

    if not isinstance(chunk, tuple) or len(chunk) != 2:
        raise ValueError(
            f"{param_name} must be a tuple of (chunk_number, total_chunks), "
            f"got {chunk}"
        )

    chunk_num, total_chunks = chunk

    if not isinstance(chunk_num, int) or not isinstance(total_chunks, int):
        raise ValueError(
            f"{param_name} must contain integers, "
            f"got ({type(chunk_num).__name__}, {type(total_chunks).__name__})"
        )

    if total_chunks < 1:
        raise ValueError(
            f"{param_name} total_chunks must be >= 1, got {total_chunks}"
        )

    if chunk_num < 1 or chunk_num > total_chunks:
        raise ValueError(
            f"{param_name} chunk_number must be between 1 and {total_chunks}, "
            f"got {chunk_num}"
        )

    return (chunk_num, total_chunks)
```

**Verification**:
```bash
uv run mypy splink/internals/chunking.py
uv run python -c "from splink.internals.chunking import validate_chunk_param; print(validate_chunk_param((1, 3), 'test'))"
```

---

#### Step 2.2: Add unit tests for chunking helpers
**File**: `tests/test_chunking.py` (NEW)

**Content**:
```python
import pytest
from splink.internals.chunking import validate_chunk_param


def test_validate_chunk_param_valid():
    assert validate_chunk_param((1, 3), "left_chunk") == (1, 3)
    assert validate_chunk_param((3, 3), "left_chunk") == (3, 3)
    assert validate_chunk_param((1, 1), "left_chunk") == (1, 1)
    assert validate_chunk_param(None, "left_chunk") is None


def test_validate_chunk_param_invalid_type():
    with pytest.raises(ValueError, match="must be a tuple"):
        validate_chunk_param([1, 3], "left_chunk")

    with pytest.raises(ValueError, match="must be a tuple"):
        validate_chunk_param(1, "left_chunk")


def test_validate_chunk_param_invalid_values():
    with pytest.raises(ValueError, match="chunk_number must be between 1 and 3"):
        validate_chunk_param((0, 3), "left_chunk")

    with pytest.raises(ValueError, match="chunk_number must be between 1 and 3"):
        validate_chunk_param((4, 3), "left_chunk")

    with pytest.raises(ValueError, match="total_chunks must be >= 1"):
        validate_chunk_param((1, 0), "left_chunk")


def test_validate_chunk_param_non_integers():
    with pytest.raises(ValueError, match="must contain integers"):
        validate_chunk_param((1.5, 3), "left_chunk")
```

**Verification**:
```bash
uv run pytest tests/test_chunking.py -v
```

---

### Phase 3: Modify Blocking SQL to Support Chunking

#### Step 3.1: Add chunk filter to `_sql_gen_where_condition()`
**File**: [splink/internals/blocking.py](splink/internals/blocking.py#L483-L500)

**Changes**:
- Add optional parameters `left_chunk` and `right_chunk` to `_sql_gen_where_condition()`
- Add optional parameter `dialect` for hash function
- Generate additional WHERE clause conditions when chunks are specified

```python
def _sql_gen_where_condition(
    link_type: backend_link_type_options,
    unique_id_cols: List[InputColumn],
    left_chunk: Tuple[int, int] | None = None,
    right_chunk: Tuple[int, int] | None = None,
    dialect: "SplinkDialect" | None = None,
) -> str:
    # ... existing code ...

    # Add chunk filtering if specified
    if left_chunk is not None:
        chunk_num, total_chunks = left_chunk
        chunk_sql = _chunk_assignment_sql(unique_id_cols, total_chunks, "l", dialect)
        where_condition += f" AND {chunk_sql} = {chunk_num}"

    if right_chunk is not None:
        chunk_num, total_chunks = right_chunk
        chunk_sql = _chunk_assignment_sql(unique_id_cols, total_chunks, "r", dialect)
        where_condition += f" AND {chunk_sql} = {chunk_num}"

    return where_condition
```

**Verification**:
```bash
uv run mypy splink/internals/blocking.py
```

---

#### Step 3.2: Update `block_using_rules_sqls()` to accept chunk parameters
**File**: [splink/internals/blocking.py](splink/internals/blocking.py#L505-L566)

**Changes**:
- Add `left_chunk`, `right_chunk`, and `dialect` parameters
- Pass through to `_sql_gen_where_condition()`

```python
def block_using_rules_sqls(
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    blocking_rules: List[BlockingRule],
    link_type: "LinkTypeLiteralType",
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
    left_chunk: Tuple[int, int] | None = None,
    right_chunk: Tuple[int, int] | None = None,
    dialect: "SplinkDialect" | None = None,
) -> list[dict[str, str]]:
```

**Verification**:
```bash
uv run mypy splink/internals/blocking.py
```

---

#### Step 3.3: Update callers of `block_using_rules_sqls()` (non-predict)
**Files**:
- [splink/internals/linker.py](splink/internals/linker.py) - `_self_link()`
- [splink/internals/estimate_u.py](splink/internals/estimate_u.py)
- [splink/internals/linker_components/inference.py](splink/internals/linker_components/inference.py) - `deterministic_link()`, `_score_missing_cluster_edges()`
- [splink/internals/linker_components/training.py](splink/internals/linker_components/training.py)
- [splink/internals/blocking_analysis.py](splink/internals/blocking_analysis.py)

**Changes**:
- Add `left_chunk=None, right_chunk=None, dialect=None` to existing calls (no functional change)

**Verification**:
```bash
uv run mypy splink/internals/linker.py
uv run mypy splink/internals/estimate_u.py
uv run mypy splink/internals/linker_components/inference.py
uv run mypy splink/internals/linker_components/training.py
uv run mypy splink/internals/blocking_analysis.py
```

---

### Phase 4: Add Chunking Methods to `LinkerInference`

We implement two functions:
1. `predict_chunk()` - Low-level function that processes a single chunk
2. `predict()` - Updated to optionally loop over chunks and combine results

#### Step 4.1: Create `predict_chunk()` method (low-level)
**File**: [splink/internals/linker_components/inference.py](splink/internals/linker_components/inference.py)

**Changes**:
- Add new method `predict_chunk()` that processes a single chunk:

```python
def predict_chunk(
    self,
    left_chunk: tuple[int, int],
    right_chunk: tuple[int, int],
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    materialise_after_computing_term_frequencies: bool = True,
    materialise_blocked_pairs: bool = True,
) -> SplinkDataFrame:
    """Process a single chunk of pairwise comparisons.

    This is a low-level method for advanced users who need fine-grained
    control over chunking (e.g., for multi-machine distribution).

    For most use cases, use `predict(left_chunks=N, right_chunks=M)` instead,
    which handles chunking automatically.

    Args:
        left_chunk: Tuple of (chunk_number, total_chunks) for left side.
            For example, (1, 3) means "chunk 1 of 3".
        right_chunk: Tuple of (chunk_number, total_chunks) for right side.
            For example, (2, 10) means "chunk 2 of 10".
        threshold_match_probability: Filter results above this probability.
        threshold_match_weight: Filter results above this match weight.
        materialise_after_computing_term_frequencies: Materialise TF table.
        materialise_blocked_pairs: Materialise blocked pairs table.

    Returns:
        SplinkDataFrame: Scored pairwise comparisons for this chunk only.

    Examples:
        ```py
        # Process chunk (1,3) × (2,10) = 1/30th of total comparisons
        df_chunk = linker.inference.predict_chunk(
            left_chunk=(1, 3),
            right_chunk=(2, 10),
        )

        # For multi-machine distribution, each machine runs different chunks
        # Machine 1: predict_chunk((1, 3), (1, 10))
        # Machine 2: predict_chunk((1, 3), (2, 10))
        # ... etc
        ```
    """
    from splink.internals.chunking import validate_chunk_param

    left_chunk = validate_chunk_param(left_chunk, "left_chunk")
    right_chunk = validate_chunk_param(right_chunk, "right_chunk")

    if left_chunk is None or right_chunk is None:
        raise ValueError("Both left_chunk and right_chunk are required")

    # ... rest of implementation uses existing predict logic with chunks ...
```

**Verification**:
```bash
uv run mypy splink/internals/linker_components/inference.py
```

---

#### Step 4.2: Update `predict()` to support automatic chunking
**File**: [splink/internals/linker_components/inference.py](splink/internals/linker_components/inference.py#L162-L295)

**Changes**:
- Add `left_chunks` and `right_chunks` parameters (note: plural, just the count)
- When specified, loop over all chunk combinations and combine results

```python
def predict(
    self,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    materialise_after_computing_term_frequencies: bool = True,
    materialise_blocked_pairs: bool = True,
    left_chunks: int | None = None,
    right_chunks: int | None = None,
) -> SplinkDataFrame:
    """Create a dataframe of scored pairwise comparisons.

    Args:
        threshold_match_probability: Filter results above this probability.
        threshold_match_weight: Filter results above this match weight.
        materialise_after_computing_term_frequencies: Materialise TF table.
        materialise_blocked_pairs: Materialise blocked pairs table.
        left_chunks: Number of chunks to partition left side into.
            When specified with right_chunks, runs chunked prediction
            which processes data in smaller pieces (useful for memory
            control or progress tracking). Results are automatically
            combined. Defaults to None (no chunking).
        right_chunks: Number of chunks to partition right side into.
            Must be specified together with left_chunks.

    Returns:
        SplinkDataFrame: Scored pairwise comparisons.

    Examples:
        ```py
        # Standard prediction (no chunking)
        df_predict = linker.inference.predict()

        # Chunked prediction - processes 12 chunks (3×4) sequentially
        # and combines results. Identical output to non-chunked version.
        df_predict = linker.inference.predict(left_chunks=3, right_chunks=4)

        # With threshold
        df_predict = linker.inference.predict(
            threshold_match_probability=0.9,
            left_chunks=5,
            right_chunks=5,
        )
        ```
    """
    # Validate chunking parameters
    if (left_chunks is None) != (right_chunks is None):
        raise ValueError(
            "left_chunks and right_chunks must both be specified or both be None"
        )

    if left_chunks is not None:
        # Chunked prediction: loop over all chunks, combine results
        return self._predict_with_chunking(
            left_chunks=left_chunks,
            right_chunks=right_chunks,
            threshold_match_probability=threshold_match_probability,
            threshold_match_weight=threshold_match_weight,
            materialise_after_computing_term_frequencies=materialise_after_computing_term_frequencies,
            materialise_blocked_pairs=materialise_blocked_pairs,
        )

    # Non-chunked prediction: existing logic
    # ... existing implementation ...
```

---

#### Step 4.3: Implement `_predict_with_chunking()` helper
**File**: [splink/internals/linker_components/inference.py](splink/internals/linker_components/inference.py)

**Changes**:
- Add private method that loops over chunks and combines:

```python
def _predict_with_chunking(
    self,
    left_chunks: int,
    right_chunks: int,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    materialise_after_computing_term_frequencies: bool = True,
    materialise_blocked_pairs: bool = True,
) -> SplinkDataFrame:
    """Internal: run all chunk combinations and combine results."""
    import logging
    logger = logging.getLogger(__name__)

    total_chunks = left_chunks * right_chunks
    chunk_results = []

    for left_n in range(1, left_chunks + 1):
        for right_n in range(1, right_chunks + 1):
            chunk_num = (left_n - 1) * right_chunks + right_n
            logger.info(
                f"Processing chunk {chunk_num}/{total_chunks} "
                f"(left={left_n}/{left_chunks}, right={right_n}/{right_chunks})"
            )

            df_chunk = self.predict_chunk(
                left_chunk=(left_n, left_chunks),
                right_chunk=(right_n, right_chunks),
                threshold_match_probability=threshold_match_probability,
                threshold_match_weight=threshold_match_weight,
                materialise_after_computing_term_frequencies=materialise_after_computing_term_frequencies,
                materialise_blocked_pairs=materialise_blocked_pairs,
            )
            chunk_results.append(df_chunk)

    # Combine all chunks via UNION ALL
    return self._combine_chunk_results(chunk_results)


def _combine_chunk_results(
    self, chunk_results: list[SplinkDataFrame]
) -> SplinkDataFrame:
    """Combine multiple chunk results into a single SplinkDataFrame."""
    if len(chunk_results) == 1:
        return chunk_results[0]

    # Build UNION ALL SQL
    union_parts = [
        f"SELECT * FROM {df.physical_name}"
        for df in chunk_results
    ]
    sql = " UNION ALL ".join(union_parts)

    pipeline = CTEPipeline(chunk_results)
    pipeline.enqueue_sql(sql, "__splink__df_predict_combined")

    combined = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

    # Clean up intermediate chunk tables
    for df in chunk_results:
        df.drop_table_from_database_and_remove_from_cache()

    return combined
```

**Verification**:
```bash
uv run mypy splink/internals/linker_components/inference.py
```

---

#### Step 4.4: Refactor core prediction logic into `_predict_core()`
**File**: [splink/internals/linker_components/inference.py](splink/internals/linker_components/inference.py)

**Changes**:
- Extract existing predict logic into `_predict_core(left_chunk, right_chunk, ...)`
- Both `predict()` (non-chunked) and `predict_chunk()` call this
- This avoids code duplication

```python
def _predict_core(
    self,
    left_chunk: tuple[int, int] | None,
    right_chunk: tuple[int, int] | None,
    threshold_match_probability: float = None,
    threshold_match_weight: float = None,
    materialise_after_computing_term_frequencies: bool = True,
    materialise_blocked_pairs: bool = True,
) -> SplinkDataFrame:
    """Core prediction logic, optionally filtered to a specific chunk."""
    # ... existing predict() implementation, but passes
    # left_chunk and right_chunk to block_using_rules_sqls()
```

**Verification**:
```bash
uv run mypy splink/internals/linker_components/inference.py
```

---

### Phase 5: Integration Tests

#### Step 5.1: Add the KEY correctness test
**File**: `tests/test_chunking_predict.py` (NEW)

This is the most important test - it verifies that chunked and non-chunked predict produce identical results:

```python
import pandas as pd
import pytest

from splink import DuckDBAPI, Linker, SettingsCreator, block_on


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "unique_id": range(100),
        "first_name": [f"name_{i % 10}" for i in range(100)],
        "surname": [f"surname_{i % 5}" for i in range(100)],
    })


@pytest.fixture
def linker(sample_data):
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[],  # Minimal for speed
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
        ],
    )
    db_api = DuckDBAPI()
    return Linker(sample_data, settings, db_api=db_api)


def test_chunked_predict_equals_non_chunked(linker):
    """THE KEY TEST: chunked and non-chunked predict must be identical.

    This is the fundamental correctness property of chunking:
    predict(left_chunks=N, right_chunks=M) must produce exactly the
    same pairs with exactly the same scores as predict() without chunking.
    """
    # Get non-chunked result
    df_no_chunk = linker.inference.predict()

    # Get chunked result (automatically combines all chunks)
    df_chunked = linker.inference.predict(left_chunks=3, right_chunks=4)

    # Convert to comparable format
    pd_no_chunk = (
        df_no_chunk.as_pandas_dataframe()
        .sort_values(["unique_id_l", "unique_id_r"])
        .reset_index(drop=True)
    )
    pd_chunked = (
        df_chunked.as_pandas_dataframe()
        .sort_values(["unique_id_l", "unique_id_r"])
        .reset_index(drop=True)
    )

    # Must have same number of rows
    assert len(pd_no_chunk) == len(pd_chunked), (
        f"Row count mismatch: {len(pd_no_chunk)} vs {len(pd_chunked)}"
    )

    # Must have identical content
    pd.testing.assert_frame_equal(pd_no_chunk, pd_chunked)
```

**Verification**:
```bash
uv run pytest tests/test_chunking_predict.py::test_chunked_predict_equals_non_chunked -v
```

---

#### Step 5.2: Add predict_chunk() tests
**File**: `tests/test_chunking_predict.py` (append)

```python
def test_predict_chunk_basic(linker):
    """predict_chunk() returns results for a single chunk"""
    df = linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(1, 3),
    )
    assert df.as_pandas_dataframe().shape[0] > 0


def test_predict_chunk_requires_both_params(linker):
    """predict_chunk() requires both left_chunk and right_chunk"""
    with pytest.raises((ValueError, TypeError)):
        linker.inference.predict_chunk(left_chunk=(1, 2))

    with pytest.raises((ValueError, TypeError)):
        linker.inference.predict_chunk(right_chunk=(1, 2))


def test_manual_chunks_equal_auto_chunked(linker):
    """Manually combining predict_chunk() results equals predict(left_chunks=...)"""
    # Auto-chunked
    df_auto = linker.inference.predict(left_chunks=2, right_chunks=2)
    auto_pairs = set(
        zip(
            df_auto.as_pandas_dataframe()["unique_id_l"],
            df_auto.as_pandas_dataframe()["unique_id_r"],
        )
    )

    # Manually chunked
    manual_pairs = set()
    for left_n in [1, 2]:
        for right_n in [1, 2]:
            df_chunk = linker.inference.predict_chunk(
                left_chunk=(left_n, 2),
                right_chunk=(right_n, 2),
            )
            chunk_pairs = set(
                zip(
                    df_chunk.as_pandas_dataframe()["unique_id_l"],
                    df_chunk.as_pandas_dataframe()["unique_id_r"],
                )
            )
            manual_pairs.update(chunk_pairs)

    assert auto_pairs == manual_pairs


def test_chunks_are_disjoint(linker):
    """Different chunks produce non-overlapping results"""
    df1 = linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=(1, 2))
    df2 = linker.inference.predict_chunk(left_chunk=(2, 2), right_chunk=(2, 2))

    set1 = set(
        zip(
            df1.as_pandas_dataframe()["unique_id_l"],
            df1.as_pandas_dataframe()["unique_id_r"],
        )
    )
    set2 = set(
        zip(
            df2.as_pandas_dataframe()["unique_id_l"],
            df2.as_pandas_dataframe()["unique_id_r"],
        )
    )

    assert set1.isdisjoint(set2)


def test_chunking_is_deterministic(linker):
    """Same chunk parameters produce identical results"""
    df1 = linker.inference.predict_chunk(left_chunk=(1, 3), right_chunk=(2, 4))
    df2 = linker.inference.predict_chunk(left_chunk=(1, 3), right_chunk=(2, 4))

    pd1 = df1.as_pandas_dataframe().sort_values(["unique_id_l", "unique_id_r"])
    pd2 = df2.as_pandas_dataframe().sort_values(["unique_id_l", "unique_id_r"])

    pd.testing.assert_frame_equal(
        pd1.reset_index(drop=True),
        pd2.reset_index(drop=True)
    )
```

**Verification**:
```bash
uv run pytest tests/test_chunking_predict.py -v
```

---

#### Step 5.3: Add validation tests
**File**: `tests/test_chunking_predict.py` (append)

```python
def test_predict_requires_both_chunk_params_or_neither(linker):
    """predict() must have both left_chunks and right_chunks or neither"""
    with pytest.raises(ValueError, match="must both be specified"):
        linker.inference.predict(left_chunks=3)

    with pytest.raises(ValueError, match="must both be specified"):
        linker.inference.predict(right_chunks=3)


def test_invalid_chunk_values(linker):
    """Invalid chunk parameters raise ValueError"""
    with pytest.raises(ValueError):
        linker.inference.predict_chunk(left_chunk=(0, 2), right_chunk=(1, 2))

    with pytest.raises(ValueError):
        linker.inference.predict_chunk(left_chunk=(3, 2), right_chunk=(1, 2))

    with pytest.raises(ValueError):
        linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=(1, 0))
```

**Verification**:
```bash
uv run pytest tests/test_chunking_predict.py -v
```

---

### Phase 6: Documentation

#### Step 6.1: Add chunking topic guide
**File**: `docs/topic_guides/performance/chunking.md` (NEW)

**Content**: Document:
- What chunking is and why it's useful
- API usage with examples
- How to iterate over all chunks
- Memory and performance considerations
- Comparison with old salting approach

**Verification**: Local docs build and review.

---

#### Step 6.2: Update predict() API documentation
**File**: Ensure predict() docstring is picked up by API docs

**Verification**: Check generated API docs.

---

### Phase 7: Optional Enhancements

#### Step 7.1: Add helper function to generate all chunk combinations
**File**: `splink/internals/chunking.py` (append)

**Content**:
```python
from typing import Iterator

def all_chunk_combinations(
    left_total: int,
    right_total: int,
) -> Iterator[tuple[tuple[int, int], tuple[int, int]]]:
    """
    Generate all (left_chunk, right_chunk) combinations for complete coverage.

    Args:
        left_total: Number of left-side chunks
        right_total: Number of right-side chunks

    Yields:
        Tuples of ((left_n, left_total), (right_n, right_total))

    Example:
        >>> list(all_chunk_combinations(2, 3))
        [((1, 2), (1, 3)), ((1, 2), (2, 3)), ((1, 2), (3, 3)),
         ((2, 2), (1, 3)), ((2, 2), (2, 3)), ((2, 2), (3, 3))]
    """
    for left_n in range(1, left_total + 1):
        for right_n in range(1, right_total + 1):
            yield ((left_n, left_total), (right_n, right_total))
```

**Verification**:
```bash
uv run python -c "from splink.internals.chunking import all_chunk_combinations; print(list(all_chunk_combinations(2, 3)))"
```

---

#### Step 7.2: Add progress reporting helper
**File**: Could be added later - just log which chunk is being processed

---

## Summary Table

| Phase | Step | File(s) | What's Added |
|-------|------|---------|--------------|
| 1 | 1.1-1.4 | dialects.py | `hash_function_name` property for each dialect |
| 2 | 2.1-2.2 | chunking.py, test_chunking.py | New file with chunk helpers + tests |
| 3 | 3.1-3.3 | blocking.py | Chunk filtering in WHERE clause |
| 4 | 4.1-4.4 | inference.py | `predict_chunk()`, `predict(left_chunks=, right_chunks=)`, `_predict_core()` |
| 5 | 5.1-5.3 | test_chunking_predict.py | Integration tests (key: chunked == non-chunked) |
| 6 | 6.1-6.2 | docs/ | Documentation |
| 7 | 7.1-7.2 | chunking.py | Optional helpers |

---

## Verification Commands Summary

After each step, run the relevant verification. Full suite after completion:

```bash
# Type checking
uv run mypy splink/internals/dialects.py
uv run mypy splink/internals/chunking.py
uv run mypy splink/internals/blocking.py
uv run mypy splink/internals/linker_components/inference.py

# Unit tests
uv run pytest tests/test_chunking.py -v
uv run pytest tests/test_chunking_predict.py -v

# THE KEY TEST: chunked must equal non-chunked
uv run pytest tests/test_chunking_predict.py::test_chunked_predict_equals_non_chunked -v

# Full DuckDB test suite (ensure no regressions)
uv run pytest -m duckdb
```

---

## Design Decisions

1. **Two-function API**:
   - `predict(left_chunks=N, right_chunks=M)` for easy use (auto-combines)
   - `predict_chunk(left_chunk=(i,N), right_chunk=(j,M))` for advanced use (multi-machine)
2. **Hash-based partitioning**: Deterministic, repeatable, no state needed between calls
3. **1-indexed chunks**: More intuitive for users ("chunk 1 of 3")
4. **Separate left/right chunking**: Maximum flexibility for different data sizes
5. **WHERE clause injection**: Minimal code changes, works with all blocking rules
6. **Key correctness property**: `predict(left_chunks=N, right_chunks=M)` MUST equal `predict()` — this is the fundamental test

---

## Migration Notes

- **No breaking changes** to existing `predict()` calls (new params have defaults)
- **Salting removal** is a prerequisite (this plan assumes it's done)
- **Spark support** can be verified/added after DuckDB implementation works

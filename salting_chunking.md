# Review of `__splink_salt` Usage in Splink

## Overview

`__splink_salt` is a column containing a random value (0-1) added to each record during vertical concatenation. It enables parallelisation of blocking rules by partitioning the join space.

## How It Works

1. **Creation**: A `random()` value is added as `__splink_salt` to each record when `salting_required=True`
2. **Usage**: `SaltedBlockingRule` adds conditions like `CEIL(l.__splink_salt * N) = k` to partition joins into N parallel tasks
3. **Cleanup**: The column is filtered out from final clustering output

## When Salting is Enabled

- **Always for DuckDB** (enables parallelisation - see [duckdb/duckdb#9710](https://github.com/duckdb/duckdb/discussions/9710))
- **Spark**: When any blocking rule has `salting_partitions > 1`

## Relevant Files

| File | Purpose |
|------|---------|
| [splink/internals/vertically_concatenate.py](splink/internals/vertically_concatenate.py#L45) | Creates the `__splink_salt` column via `random()` |
| [splink/internals/blocking.py](splink/internals/blocking.py#L292-L330) | `SaltedBlockingRule` class; uses salt in join condition |
| [splink/internals/settings.py](splink/internals/settings.py#L665-L675) | `salting_required` property determining when salting is enabled |
| [splink/internals/linker_components/clustering.py](splink/internals/linker_components/clustering.py#L158) | Filters out `__splink_salt` from final output |

## Key Code Snippets

**Salt creation** ([vertically_concatenate.py](splink/internals/vertically_concatenate.py#L44-L46)):
```python
if salting_required:
    salt_sql = ", random() as __splink_salt"
```

**Salt condition in blocking** ([blocking.py](splink/internals/blocking.py#L313-L314)):
```python
def _salting_condition(self, salt):
    return f"AND ceiling(l.__splink_salt * {self.salting_partitions}) = {salt + 1}"
```

**Salting decision logic** ([settings.py](splink/internals/settings.py#L665-L675)):
```python
@property
def salting_required(self):
    if self._sql_dialect_str == "duckdb":
        return True
    for br in self._blocking_rules_to_generate_predictions:
        if isinstance(br, SaltedBlockingRule):
            return True
    return False
```

## Replacing Salting with Chunking (Splink 5)

### The Problem with Salting

Salting was introduced to help parallelise blocking joins and handle skew (e.g. many "John Smith" records). However:
- It's Spark-specific in design
- For DuckDB, it's always enabled but doesn't solve memory/scaling issues for very large linkages
- Users still hit OOM errors on large jobs (>1bn rows) despite salting

### Chunking: A Better Approach

**Chunking** splits `predict()` into independent sub-jobs by partitioning both left and right sides of the join.

**Proposed API** (Splink 5):
```python
# Run on 1/30th of the data (chunk 1 of 3 on left × chunk 3 of 10 on right)
linker.inference.predict(left_chunk=(1, 3), right_chunk=(3, 10))
```

- `left_chunk=(1, 3)` means: partition the left dataframe into 3 equal chunks, take chunk 1
- `right_chunk=(3, 10)` means: partition the right dataframe into 10 equal chunks, take chunk 3
- This call processes 1/30th of the full comparison space

**Mechanism**:

Splink's unique ID is often composite (e.g. `source_dataset || '-__-' || unique_id` for link jobs). The chunk assignment must hash this composite key:

```sql
-- Assign chunk via hash of composite unique ID + modulo
(hash(source_dataset || '-__-' || unique_id) % num_chunks) + 1

-- For dedupe (single dataset), just hash the unique_id
(hash(unique_id) % num_chunks) + 1
```

This reuses the existing `_composite_unique_id_from_nodes_sql()` helper from [unique_id_concat.py](splink/internals/unique_id_concat.py).

To run a full linkage across 30 chunks, you'd call `predict()` 30 times (or distribute across machines):
- `left_chunk=(1,3), right_chunk=(1,10)` ... `left_chunk=(1,3), right_chunk=(10,10)`
- `left_chunk=(2,3), right_chunk=(1,10)` ... etc.

### Why Chunking Replaces Salting

| Aspect | Salting | Chunking |
|--------|---------|----------|
| Parallelisation | ✓ | ✓ |
| Handles skew | ✓ | ✓ |
| Progress feedback | ✗ | ✓ (e.g. "10% complete") |
| Memory control | ✗ | ✓ (run sequentially on small machine) |
| Multi-machine distribution | ✗ | ✓ (assign chunks to different machines) |
| Time estimation | ✗ | ✓ (run 1 chunk to estimate total time) |
| Both-sided partitioning | ✗ | ✓ (reduces peak memory further) |

Chunking provides all benefits of salting plus additional capabilities, making salting redundant. Splink 5 will remove the `salting_partitions` parameter from blocking rules in favour of `left_chunk`/`right_chunk` arguments on `predict()`.



Here’s an updated plan that folds in all the recommendations and keeps public naming as stable and consistent as possible.

---

# Revised Plan: Simplify Caching in Splink (Public API–Friendly)

## Rationale

Right now Splink has two conflated caching behaviours:

### 1. Implicit SQL Hash-Based Caching (TO REMOVE)

* Hashes SQL pipelines and caches results in the DB layer
* Rarely hits in realistic user workflows
* Hard to reason about – users don’t know when it hits/misses
* Causes subtle bugs when settings/data change but cache isn’t invalidated correctly
* ~500 lines of complex, cross-cutting code

### 2. Explicit Table Caching via `_intermediate_table_cache` (TO KEEP, BUT TIGHTEN)

* Caches a small set of well-known “intermediate” tables:

  * Concatenated inputs with TF columns
  * TF lookup tables
  * Predictions table
* Populated via deliberate workflows (compute tables or register precomputed ones)
* Easy to explain / reason about: user knows what’s cached

We want to:

* Remove the *implicit* query-hash caching hidden in the DB layer
* Keep a *tiny, explicit* table cache that’s easy to understand and control
* Do that without unnecessarily renaming or breaking the public API

---

## Goals

1. **Remove** SQL hash-based caching from the DB layer (`database_api`, etc.)
2. **Keep and simplify** `_intermediate_table_cache` as a small, named-table cache:

   * Only a handful of blessed keys, no arbitrary hash keys
3. **Ensure** only `LinkerTableManagement` writes to `_intermediate_table_cache`
4. **Keep** existing public method names wherever possible

   * e.g. keep `compute_df_concat_with_tf`, `register_table_input_nodes_concat_with_tf`, `invalidate_cache`
5. **Provide** simple patterns for:

   * Computing and caching intermediate tables
   * Registering precomputed tables
   * Optional power-user overrides (`df_concat_with_tf` parameters)
6. **Preserve semantics** of `invalidate_cache()` (including dropping tables + bumping `_cache_uid`)

---

## Target User Experience

Nothing surprising from the outside; caching is predictable and explicit.

```python
# Simple case – just works; cache is populated on first use
linker.training.estimate_u_using_random_sampling()
linker.inference.predict()  # Will compute and cache df_concat_with_tf as needed

# Explicit: compute & inspect intermediate table
df_concat_with_tf = linker.table_management.compute_df_concat_with_tf()
df_concat_with_tf.as_pandas_dataframe().to_parquet("checkpoint.parquet")

# Later, reuse precomputed intermediate table (possibly in a new process)
df_concat_with_tf = pd.read_parquet("checkpoint.parquet")
linker.table_management.register_table_input_nodes_concat_with_tf(df_concat_with_tf)
linker.inference.predict()  # Uses registered / cached df_concat_with_tf

# Power user: bypass cache entirely using explicit argument
my_custom_df_concat = ...
linker.inference.predict(df_concat_with_tf=my_custom_df_concat)
```

(If we decide to add nicer alias names like `register_input_records_with_tf`, they’ll just call the existing methods internally; the public “legacy” names remain.)

---

## Blessed Cache Keys

`_intermediate_table_cache` will *only* ever hold these keys:

| Key                           | Description                             | Set By                                                                       |
| ----------------------------- | --------------------------------------- | ---------------------------------------------------------------------------- |
| `__splink__df_concat_with_tf` | Concatenated input data with TF columns | `compute_df_concat_with_tf()`, `register_table_input_nodes_concat_with_tf()` |
| `__splink__df_tf_{column}`    | TF lookup table for a column            | `compute_tf_table()`, `register_term_frequency_lookup()`                     |
| `__splink__df_predict`        | Predictions table                       | `register_table_predict()`                                                   |

No SQL hashes, no ad-hoc keys.

This invariant is enforced by convention and code comments on `Linker`: **only `LinkerTableManagement` writes to this cache**.

---

## Implementation Plan

### Phase 0: Public API Stability

**Principles:**

* Do **not** remove or rename existing public methods unless absolutely necessary.
* If we add new names:

  * Implement them as thin aliases over existing methods.
  * Document them as ergonomic helpers, not replacements.
* Keep `invalidate_cache()` behaviour the same (bump `_cache_uid`, delete Splink tables, clear cache).

---

### Phase 1: Make `compute_df_concat_with_tf` populate the cache ✅ DONE

**File:** `splink/internals/linker_components/table_management.py`

**Current behaviour (conceptual):**

* `compute_df_concat_with_tf()`:

  * Computes concatenated input table with TF columns.
  * Returns a `SplinkDataFrame`.
  * Does *not* reliably populate `_intermediate_table_cache`.

**New behaviour:**

* `compute_df_concat_with_tf()`:

  * Computes the table.
  * Sets `templated_name` to `__splink__df_concat_with_tf`.
  * Stores it in `_intermediate_table_cache["__splink__df_concat_with_tf"]`.
  * Returns the `SplinkDataFrame`.

Example:

```python
def compute_df_concat_with_tf(self, cache: bool = True) -> SplinkDataFrame:
    """Compute concatenated input records with term frequency columns.

    If cache=True (default), this stores the result in the intermediate
    table cache under the key "__splink__df_concat_with_tf", so other
    methods (e.g. predict, training) can reuse it.
    """
    pipeline = CTEPipeline()
    df = _compute_df_concat_with_tf(self._linker, pipeline)

    df.templated_name = "__splink__df_concat_with_tf"

    if cache:
        self._linker._intermediate_table_cache["__splink__df_concat_with_tf"] = df

    return df
```

* No renaming of the public method.
* Optional `cache=False` gives a “compute but don’t touch cache” escape hatch if needed.

**Verification:**

```bash
uv run pytest -m duckdb
uv run mypy splink/internals/linker_components/table_management.py
```

---

### Phase 2: Ensure `compute_tf_table` & `register_*` write to cache correctly ✅ DONE

**File:** `splink/internals/linker_components/table_management.py`

#### 2.1 `compute_tf_table` (keep name)

* Confirm it follows this pattern:

```python
tf_tablename = f"__splink__df_tf_{column_name}"
cache = self._linker._intermediate_table_cache

if tf_tablename in cache:
    tf_df = cache.get_with_logging(tf_tablename)
else:
    # Compute TF table via SQL/pipeline
    tf_df = ...
    self._linker._intermediate_table_cache[tf_tablename] = tf_df
    tf_df.templated_name = tf_tablename

return tf_df
```

* The key point: TF tables are explicitly cached under the blessed name.

#### 2.2 `register_table_input_nodes_concat_with_tf`

* Keep the existing name (public API).
* Make sure it registers the table and writes to the cache under the standard key:

```python
def register_table_input_nodes_concat_with_tf(
    self, input_data: AcceptableInputTableType, overwrite: bool = False
) -> SplinkDataFrame:
    """Register a pre-computed df_concat_with_tf table.

    This is the standard way to reuse a precomputed df_concat_with_tf
    (e.g. from parquet) in another process.
    """
    table_name_physical = "__splink__df_concat_with_tf_" + self._linker._cache_uid
    splink_df = self.register_table(
        input_data, table_name_physical, overwrite=overwrite
    )
    splink_df.templated_name = "__splink__df_concat_with_tf"
    self._linker._intermediate_table_cache["__splink__df_concat_with_tf"] = splink_df
    return splink_df
```

* If we later add a nicer alias (e.g. `register_input_records_with_tf`), it should just call this method.

#### 2.3 `register_term_frequency_lookup` & `register_table_predict`

* `register_term_frequency_lookup`:

  * Keep name.
  * Make sure it writes to cache key `__splink__df_tf_{column}`.

* `register_table_predict`:

  * Keep name.
  * Make sure it writes to `__splink__df_predict`.

**Verification:**

```bash
uv run pytest -m duckdb
```

---

### Phase 3: Centralise cache reads via a Linker helper ✅ DONE

**File:** `splink/internals/linker.py` (or equivalent core linker module)

Add a private helper on `Linker`:

```python
def _get_df_concat_with_tf(
    self, df_concat_with_tf: SplinkDataFrame | None = None
) -> SplinkDataFrame:
    """Resolve df_concat_with_tf according to the standard precedence:

    1. If df_concat_with_tf argument is provided, use it directly.
    2. Else if a cached df_concat_with_tf exists, use it.
    3. Else compute df_concat_with_tf via table_management and cache it.
    """
    if df_concat_with_tf is not None:
        return df_concat_with_tf

    cache = self._intermediate_table_cache

    key = "__splink__df_concat_with_tf"
    if key in cache:
        return cache.get_with_logging(key)

    # Will compute and cache via Phase 1 logic
    return self.table_management.compute_df_concat_with_tf()
```

Then, in inference/training/clustering methods that need `df_concat_with_tf`, use:

```python
def predict(
    self,
    ...,
    df_concat_with_tf: SplinkDataFrame | None = None,
):
    df_concat_with_tf = self._linker._get_df_concat_with_tf(df_concat_with_tf)
    ...
```

This avoids duplicating cache-resolution logic everywhere and standardises behaviour.

**Verification:**

```bash
uv run pytest -m duckdb
```

---

### Phase 4: Restrict cache writes to `table_management.py` ✅ DONE (Partial)

**Note:** The functions `compute_df_concat_with_tf` and `compute_df_concat` in `vertically_concatenate.py` still write directly to the cache for internal use. This is acceptable as:
1. These writes are for the "blessed" keys only (`__splink__df_concat_with_tf`, `__splink__df_concat`)
2. The public API is now through `LinkerTableManagement.compute_df_concat_with_tf()`
3. The problematic SQL-hash-based caching (in database_api.py) will be removed in Phase 5

**Goal:** `_intermediate_table_cache` writes only occur in `LinkerTableManagement` (and maybe very tightly-scoped internal helpers it owns).

**Steps:**

```bash
grep -rn "_intermediate_table_cache\[" splink/
```

* Expected writes after refactor:

  * `compute_df_concat_with_tf`
  * `compute_tf_table`
  * `register_table_input_nodes_concat_with_tf`
  * `register_term_frequency_lookup`
  * `register_table_predict`
* Remove / refactor any other writes, e.g. in:

  * `vertically_concatenate.py`
  * `database_api.py`
  * inference / training files directly writing to the cache

If any non-table_management code needs to “cache” a table, it should instead call one of the `LinkerTableManagement` methods.

**Verification:**

```bash
uv run pytest -m duckdb
```

---

### Phase 5: Remove SQL hash-based caching from DB layer ✅ DONE

#### 5.1 Identify & remove hash-based caching

**File:** `splink/internals/database_api.py`

Search for:

* `sql_to_splink_dataframe_checking_cache`
* `cache_key`, SQL hashing
* `use_cache` flags / parameters

**Changes:**

* Remove `sql_to_splink_dataframe_checking_cache` (or fold its non-caching bits into a simpler helper).
* Remove `use_cache` parameters and logic from methods.
* Simplify to “execute SQL and wrap result in `SplinkDataFrame`”.

#### 5.2 Simplify `sql_pipeline_to_splink_dataframe`

Conceptual before:

```python
def sql_pipeline_to_splink_dataframe(self, pipeline, use_cache=True):
    if use_cache:
        cache_key = hash(pipeline.sql)
        if cache_key in self._cache:
            return self._cache[cache_key]

    result = self._execute(pipeline)

    if use_cache:
        self._cache[cache_key] = result

    return result
```

After:

```python
def sql_pipeline_to_splink_dataframe(self, pipeline) -> SplinkDataFrame:
    """Execute the SQL pipeline and return a SplinkDataFrame.

    This method does not perform any caching based on the SQL text.
    """
    return self._execute(pipeline)
```

#### 5.3 Simplify `CacheDictWithLogging`

**File:** `splink/internals/cache_dict_with_logging.py`

* Keep it as a thin wrapper around `dict` + logging (counts of gets/puts).
* Remove any SQL-specific or hash-based logic.
* Optionally keep `queries_executed` counters for debugging, but they should have no behavioural effect.

**Verification:**

```bash
uv run pytest -m duckdb
uv run mypy splink/internals/database_api.py
```

---

### Phase 6: Invalidation semantics ✅ DONE

We keep the existing behaviour of `invalidate_cache` and optionally add more granular helpers.

#### 6.1 Preserve `invalidate_cache()` behaviour

Wherever `invalidate_cache()` currently lives (likely on `Linker` or `LinkerTableManagement`), ensure it still:

1. Bumps `_cache_uid` (so new tables get new physical names)
2. Asks `db_api` to drop Splink-created tables
3. Clears `_intermediate_table_cache`

Conceptually:

```python
def invalidate_cache(self) -> None:
    """Invalidate all Splink intermediate tables and cache.

    Keeps legacy semantics: new cache uid, drop tables, clear dict.
    """
    self._cache_uid = ascii_uid(8)
    self._db_api.delete_tables_created_by_splink_from_db()
    self._intermediate_table_cache.invalidate_cache()
```

If you want a clearer alias name like `invalidate_all_cached_tables`, implement it as:

```python
def invalidate_all_cached_tables(self) -> None:
    self.invalidate_cache()
```

#### 6.2 (Optional) Targeted invalidation helpers

If useful, add lighter-weight helpers that only touch the cache dict (and, optionally, specific physical tables):

```python
def invalidate_df_concat_with_tf(self) -> None:
    cache = self._linker._intermediate_table_cache
    cache.pop("__splink__df_concat_with_tf", None)
    # Optional: drop its physical table from DB if desired.

def invalidate_tf_table(self, column_name: str) -> None:
    cache = self._linker._intermediate_table_cache
    key = f"__splink__df_tf_{column_name}"
    cache.pop(key, None)
    # Optional: drop the physical table.
```

These are *additive* conveniences and do not change or weaken the behaviour of the existing `invalidate_cache()` method.

**Verification:**

```bash
uv run pytest -m duckdb
uv run mypy splink/internals/linker_components/table_management.py
```

---

### Phase 7: Tests & cleanup ✅ DONE

#### 7.1 Remove / update old caching tests

**Files (approx):**

* `tests/test_caching.py`
* `tests/test_caching_tables.py`
* Any tests referring to SQL-hash caching or `use_cache` flags

Changes:

* Remove tests that assert query-hash-based caching behaviour.
* Add / keep tests that:

  * `compute_df_concat_with_tf()` populates `_intermediate_table_cache`.
  * `register_table_input_nodes_concat_with_tf()` and `register_term_frequency_lookup()` populate cache correctly.
  * `predict()` and training methods:

    * Use explicit `df_concat_with_tf` if supplied.
    * Fall back to cached version when available.
    * Compute-and-cache via `compute_df_concat_with_tf()` when needed.

#### 7.2 Sanity test of intended UX

In a smoke test / notebook:

```python
linker = Linker(df, settings, db_api)

# 1. compute_df_concat_with_tf populates cache
df_concat = linker.table_management.compute_df_concat_with_tf()
assert "__splink__df_concat_with_tf" in linker._intermediate_table_cache

# 2. predict() reuses cached table
predictions = linker.inference.predict()
assert predictions is not None

# 3. invalidate_cache clears cache and drops tables
linker.invalidate_cache()
assert "__splink__df_concat_with_tf" not in linker._intermediate_table_cache

# 4. register workflow
df_concat2 = linker.table_management.compute_df_concat_with_tf()
df_concat2_pd = df_concat2.as_pandas_dataframe()
df_concat2_pd.to_parquet("saved_df_concat_with_tf.parquet")

linker2 = Linker(df, settings, db_api)
precomputed = pd.read_parquet("saved_df_concat_with_tf.parquet")
linker2.table_management.register_table_input_nodes_concat_with_tf(precomputed)
assert "__splink__df_concat_with_tf" in linker2._intermediate_table_cache

predictions2 = linker2.inference.predict()
assert predictions2 is not None
```

---

## Summary

* We **remove** the confusing SQL-hash-based caching at the DB layer.
* We **keep** a small set of explicitly named cached tables in `_intermediate_table_cache`.
* Only `LinkerTableManagement` writes to the cache; inference/training read via a single helper `_get_df_concat_with_tf`.
* We **do not** rename existing public methods unnecessarily:

  * `compute_df_concat_with_tf`, `register_table_input_nodes_concat_with_tf`, `register_term_frequency_lookup`, `invalidate_cache` all remain.
* We preserve existing invalidation semantics and optionally add small, clearer convenience aliases and targeted invalidation helpers.

From a user’s perspective, existing code keeps working, but the caching story becomes simpler, more predictable, and much easier to reason about.

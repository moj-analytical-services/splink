# PR Review: optimise_train_u vs splinkdataframes_everywhere

## High-level aim (as I understand it)
This PR changes u-probability estimation to be:

1. **More memory-friendly** by estimating u **comparison-by-comparison** rather than across all comparisons in one wide pipeline.
2. **More statistically efficient** by using **count-based convergence** (per comparison level) so we can stop sampling early once each level has a minimum number of observations.
3. **More observable** by returning diagnostics for how many pairs/levels were sampled and whether the run converged.

Overall: the direction is good and aligns well with the goal of improving scalability without sacrificing correctness.

---

## What changed

| File | Lines | Summary |
|------|-------|---------|
| `splink/internals/estimate_u.py` | +345 / -51 | Core refactor: per-comparison convergence loop, dataclasses for diagnostics |
| `splink/internals/comparison_vector_values.py` | +17 / -6 | Parameterised blocked pairs table name |
| `tests/test_u_train.py` | +255 / -1 | 6 new convergence tests |

### splink/internals/estimate_u.py
- Introduces `LevelConvergenceInfo` and `ComparisonConvergenceInfo` diagnostics dataclasses.
- Adds `_estimate_u_for_comparison_with_convergence(...)`:
  - Creates disjoint batches of blocked pairs using a generated `__batch_rand__` column.
  - Iteratively processes batches until each non-null level reaches `min_count` or all pairs are exhausted.
  - Computes u-probabilities from accumulated counts.
- Refactors `estimate_u_values(...)` to:
  - Materialise split sample tables for `link_only` mode with 2 input tables.
  - Materialise `__splink__blocked_id_pairs` (so debug/tests can inspect it).
  - Materialise `__splink__blocked_id_pairs_with_rand`.
  - Loop comparisons and call the per-comparison convergence routine.
  - Append trained u values into each comparison level's trained probs.
  - **Return** `List[ComparisonConvergenceInfo]`.
- **Removes dependency** on `compute_new_parameters_sql` and `compute_proportions_for_new_parameters` from the EM module—u is now computed directly from counts.

### splink/internals/comparison_vector_values.py
- Generalises `compute_comparison_vector_values_from_id_pairs_sqls(...)` with an optional `blocked_id_pairs_table` parameter so batching can feed comparison vector computation without clobbering the canonical `__splink__blocked_id_pairs`.
- Fixes typo "alises" → "aliases".
- Fixes duplicate assignment (`sql = sql = ...` → `sql = ...`).

### tests/test_u_train.py
- Adds a set of convergence-focused tests that validate:
  - Diagnostics structure is returned.
  - Early termination occurs when `min_count` is small.
  - `min_count` influences behaviour.
  - Multi-comparison runs work.
  - u-probabilities sum to ~1.
  - Link-only mode works.

---

## What's strong

- **Clear algorithmic improvement**: the convergence loop is easy to follow and avoids the earlier "big all-comparisons" pipeline approach.
- **Good boundary around new behaviour**: convergence logic lives in `_estimate_u_for_comparison_with_convergence`, keeping `estimate_u_values` readable.
- **Fix for earlier test/debug regression**: preserving the materialised `__splink__blocked_id_pairs` is important for the existing debug-mode tests.
- **Good practical clamp**: clamping to `M_U_CLAMP_MIN` avoids downstream failures when a level is unobserved.
- **Test coverage**: the new tests validate the key behaviour (diagnostics + convergence) and run fast.
- **Removed EM dependency**: the old code imported `compute_new_parameters_sql` and `compute_proportions_for_new_parameters` from EM. The new approach computes u directly from counts, which is conceptually cleaner for this use case.

---

## Key concerns / things to reconsider

### 1) Reproducibility when `seed` is provided
`seed` currently controls ordering/sampling of `__splink__df_concat`, but batching uses `random()` to generate `__batch_rand__`.

- Many engines' `random()` is not seed-stable across runs unless you explicitly seed the RNG for the session/connection.
- That means **two runs with the same `seed` could still produce different u estimates**, even if the sampled rows are the same.

**Suggestion:** consider deriving `__batch_rand__` from a stable hash of `(join_key_l, join_key_r)` (or the underlying ids) and the provided `seed`, e.g. something like `hash(join_key_l || join_key_r || seed)` mapped to [0,1). This is both deterministic and portable.

### 2) Semantics of `max_pairs` vs convergence
Docstring states "…or when `max_pairs` worth of pairs have been processed", but the implementation actually processes up to **all pairs in `__splink__blocked_id_pairs`**, where that table size is only *approximately* controlled by `max_pairs` through upstream sampling.

That's probably fine in practice, but the wording is slightly misleading.

**Suggestion:**
- Either tighten the contract (rename/clarify: `max_pairs` controls the *targeted* pair volume via row-sampling), or
- enforce a hard cap by stopping once `total_pairs >= max_pairs`.

### 3) Materialising `__splink__blocked_id_pairs_with_rand`
Even though you're sampling rows to target `~max_pairs` candidate pairs, `__splink__blocked_id_pairs_with_rand` is still a full materialisation of that candidate set.

This is probably acceptable given the existing design (we already materialise blocked pairs), but it does mean the convergence change doesn't reduce the *storage* footprint, only the per-comparison compute footprint.

**Suggestion:** if storage/materialisation becomes an issue on some backends, consider alternatives:
- avoid adding a full random column and instead partition by `row_number()` windows if supported,
- or generate batches from deterministic hashing without materialising the extra column.

### 4) Public-ish API change: `estimate_u_values` return type
`estimate_u_values(...)` used to return `None` and now returns a list of diagnostics.

Even though it's in `internals`, it's imported in tests and could be imported by users or other internal code. Returning diagnostics is useful, but the change is still a behaviour shift.

**Suggestion:**
- Either keep the old return type (`None`) and optionally log/attach diagnostics elsewhere, or
- make diagnostics return opt-in (e.g., `return_diagnostics: bool = False`).

### 5) Each comparison re-joins sample tables to blocked pairs batch
Inside `_estimate_u_for_comparison_with_convergence`, each batch iteration:
1. Filters `__splink__blocked_id_pairs_with_rand` to a range
2. Joins back to `df_sample` (or split left/right tables) to get column values
3. Computes gamma for **one** comparison

This means if you have 5 comparisons, you do 5× as many joins to the sample tables as before (old code computed all gammas in one pass). For large datasets with many comparisons, this could be slower overall despite early termination.

**Suggestion:** consider a hybrid approach:
- Compute comparison vectors for **all** comparisons in one batch pass
- Track convergence per-comparison, but share the join work
- This would preserve the memory benefit (don't need the wide EM parameter table) while avoiding repeated join overhead

### 6) The convergence benefit is comparison-specific but pairs are shared
All comparisons operate on the same blocked pairs. If comparison A converges in 10% of pairs but comparison B needs 80%, the current design still processes 80% for B even though A is "done".

That's fine and correct, but worth noting: the early termination benefit is **per-comparison**, not global. The slowest-to-converge comparison determines total runtime for that comparison's estimation, but at least fast comparisons finish quickly.

---

## Tests: what's good and what could be improved

### What's good
- The new tests directly exercise the convergence behaviour and validate both correctness and early stopping.
- The tests are fast and mostly deterministic in practice.
- Good coverage of edge cases: link_only mode, multiple comparisons, u-probabilities summing to 1.

### Possible improvements

1. **`test_convergence_min_count_parameter` has a weak `else: pass` branch:**
   ```python
   if diag_high.converged:
       for level in diag_high.levels:
           assert level.count >= 200
   else:
       # If it didn't converge, it should have processed all pairs
       pass  # <-- This asserts nothing!
   ```
   Consider asserting: `assert diag_high.total_pairs_sampled == expected_total` or `assert diag_high.converged is False`.

2. **No test for the `seed` parameter with convergence:**
   Given the reproducibility concern above, it would be valuable to have a test that:
   - Runs `estimate_u_values` twice with the same seed
   - Asserts the diagnostics (or at least u-probabilities) are identical

3. **Some tests use DuckDB directly, others use `test_helpers`:**
   This is fine, but the direct DuckDB tests won't run on other backends. Consider whether the convergence tests should be dialect-agnostic via `@mark_with_dialects_excluding()`.

4. **No test for the edge case where a level has 0 observations:**
   The code clamps to `M_U_CLAMP_MIN`, but there's no test verifying this path works correctly.

---

## Style / maintainability notes

1. **`logger.log(7, ...)` is unusual:** Level 7 is between `DEBUG` (10) and `NOTSET` (0). If this is meant to be extra-verbose debug output, consider using a named level or just `logger.debug(...)` with a conditional.

2. **The helper function has 14 parameters:**
   `_estimate_u_for_comparison_with_convergence(...)` takes 14 arguments. This is borderline unwieldy. Consider:
   - Grouping related params into a small dataclass/namedtuple (e.g., `SampleContext` holding `df_sample`, `df_sample_left`, `df_sample_right`, `input_tablename_sample_l`, `input_tablename_sample_r`)
   - Or passing the `settings_obj` / `training_linker` and extracting what's needed inside

3. **Duplicate column-building logic:**
   The code builds `cols_for_blocking` and `cols_for_cv` inside `_estimate_u_for_comparison_with_convergence`. This is similar to what `Settings` methods do. Could potentially reuse existing methods more directly.

4. **Typo in commit message:** "estimaet u" → "estimate u" (minor, but worth fixing before merge)

---

## Alternative design worth considering

**Batch-then-aggregate instead of iterate-until-converge:**

Instead of iteratively growing batches until convergence, an alternative is:
1. Compute all comparison vectors once (as before), but only for **one comparison at a time**
2. Aggregate counts in a single pass
3. Use the full sample (no batching) but with the per-comparison memory benefit

This trades off early-termination efficiency for simpler code. The current design is more sophisticated but adds complexity. Whether the early termination matters depends on:
- How often levels converge quickly (common for low-cardinality columns)
- How expensive the join-and-compute step is per batch

If early termination rarely triggers in practice, the simpler design might be preferable.

---

## Overall recommendation
**Approve with suggestions**.

The implementation achieves the stated goals and test coverage is solid. Key items before merge:

1. **Must address:** The `seed` reproducibility story—either document that seed doesn't guarantee identical results with batching, or make batching deterministic.
2. **Should address:** Strengthen the `else: pass` test case.
3. **Nice to have:** Consider the hybrid approach (shared join, per-comparison gamma) if benchmarking shows the repeated joins are costly.

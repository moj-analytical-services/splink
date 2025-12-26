# PR Review: optimise_train_u vs splinkdataframes_everywhere

## High-level aim (as I understand it)
This PR changes u-probability estimation to be:

1. **More memory-friendly** by estimating u **comparison-by-comparison** rather than across all comparisons in one wide pipeline.
2. **More statistically efficient** by using **count-based convergence** (per comparison level) so we can stop sampling early once each level has a minimum number of observations.
3. **More observable** by returning diagnostics for how many pairs/levels were sampled and whether the run converged.

Overall: the direction is good and aligns well with the goal of improving scalability without sacrificing correctness.

---

## What changed

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
  - Append trained u values into each comparison level’s trained probs.
  - **Return** `List[ComparisonConvergenceInfo]`.

### splink/internals/comparison_vector_values.py
- Generalises `compute_comparison_vector_values_from_id_pairs_sqls(...)` with an optional `blocked_id_pairs_table` parameter so batching can feed comparison vector computation without clobbering the canonical `__splink__blocked_id_pairs`.

### tests/test_u_train.py
- Adds a set of convergence-focused tests that validate:
  - Diagnostics structure is returned.
  - Early termination occurs when `min_count` is small.
  - `min_count` influences behaviour.
  - Multi-comparison runs work.
  - u-probabilities sum to ~1.
  - Link-only mode works.

---

## What’s strong

- **Clear algorithmic improvement**: the convergence loop is easy to follow and avoids the earlier “big all-comparisons” pipeline approach.
- **Good boundary around new behaviour**: convergence logic lives in `_estimate_u_for_comparison_with_convergence`, keeping `estimate_u_values` readable.
- **Fix for earlier test/debug regression**: preserving the materialised `__splink__blocked_id_pairs` is important for the existing debug-mode tests.
- **Good practical clamp**: clamping to `M_U_CLAMP_MIN` avoids downstream failures when a level is unobserved.
- **Test coverage**: the new tests validate the key behaviour (diagnostics + convergence) and run fast.

---

## Key concerns / things to reconsider

### 1) Reproducibility when `seed` is provided
`seed` currently controls ordering/sampling of `__splink__df_concat`, but batching uses `random()` to generate `__batch_rand__`.

- Many engines’ `random()` is not seed-stable across runs unless you explicitly seed the RNG for the session/connection.
- That means **two runs with the same `seed` could still produce different u estimates**, even if the sampled rows are the same.

**Suggestion:** consider deriving `__batch_rand__` from a stable hash of `(join_key_l, join_key_r)` (or the underlying ids) and the provided `seed`, e.g. something like `hash(join_key_l || join_key_r || seed)` mapped to [0,1). This is both deterministic and portable.

### 2) Semantics of `max_pairs` vs convergence
Docstring states “…or when `max_pairs` worth of pairs have been processed”, but the implementation actually processes up to **all pairs in `__splink__blocked_id_pairs`**, where that table size is only *approximately* controlled by `max_pairs` through upstream sampling.

That’s probably fine in practice, but the wording is slightly misleading.

**Suggestion:**
- Either tighten the contract (rename/clarify: `max_pairs` controls the *targeted* pair volume via row-sampling), or
- enforce a hard cap by stopping once `total_pairs >= max_pairs`.

### 3) Materialising `__splink__blocked_id_pairs_with_rand`
Even though you’re sampling rows to target `~max_pairs` candidate pairs, `__splink__blocked_id_pairs_with_rand` is still a full materialisation of that candidate set.

This is probably acceptable given the existing design (we already materialise blocked pairs), but it does mean the convergence change doesn’t reduce the *storage* footprint, only the per-comparison compute footprint.

**Suggestion:** if storage/materialisation becomes an issue on some backends, consider alternatives:
- avoid adding a full random column and instead partition by `row_number()` windows if supported,
- or generate batches from deterministic hashing without materialising the extra column.

### 4) Public-ish API change: `estimate_u_values` return type
`estimate_u_values(...)` used to return `None` and now returns a list of diagnostics.

Even though it’s in `internals`, it’s imported in tests and could be imported by users or other internal code. Returning diagnostics is useful, but the change is still a behaviour shift.

**Suggestion:**
- Either keep the old return type (`None`) and optionally log/attach diagnostics elsewhere, or
- make diagnostics return opt-in (e.g., `return_diagnostics: bool = False`).

---

## Tests: what’s good and what could be improved

### What’s good
- The new tests directly exercise the convergence behaviour and validate both correctness and early stopping.
- The tests are fast and mostly deterministic in practice.

### Possible improvements
- `test_convergence_min_count_parameter` has an `else: pass` branch for non-convergence. This doesn’t assert anything useful.
  - Consider asserting something about the non-converged case (e.g. `diag_high.converged is False` implies `batches_processed` hit the maximum and/or all pairs were exhausted).
- Some tests construct a `Linker` directly (DuckDB only) while others use the `test_helpers` harness. That’s fine, but consistency could improve readability.
- If the batch randomisation becomes deterministic via hashing, tests could assert stronger invariants about `batches_processed` and `total_pairs_sampled`.

---

## Style / maintainability notes

- The per-comparison loop in `estimate_u_values` is now significantly longer. It’s still readable, but it might be worth extracting the "setup" phases (sampling, optional split, blocking/materialisation) into small helpers to keep the main routine as a high-level story.
- Logging level `logger.log(7, ...)` is a bit unusual. If the project has a convention for custom verbose levels, fine; if not, a normal `debug` might be clearer.

---

## Overall recommendation
**Approve with suggestions**.

The implementation achieves the intent and the test suite coverage is much improved. The main follow-up I’d want before merge is addressing (or explicitly accepting) the **seed/determinism** story for `__batch_rand__`, because that’s the sort of subtle reproducibility issue that can surprise users.

# Proposal: Switch from Multiplicative Bayes Factors to Additive Match Weights

**Issue reference:** [#1889](https://github.com/moj-analytical-services/splink/issues/1889) - "can't take logarithm of zero"

## Executive Summary

Splink currently computes match scores by **multiplying Bayes Factors** in SQL and then taking the logarithm. This approach is numerically unstable and causes `log(0)` errors. We propose switching to **additive match weights** computed directly in log-space, which eliminates these numerical issues entirely.

---

## Background: The Mathematical Relationship

### Bayes Factors and Match Weights

In the Fellegi-Sunter model, evidence for or against a match is quantified using **m** and **u** probabilities:

- $m = \Pr(\text{scenario} \mid \text{match})$ — probability of observing this comparison outcome among true matches
- $u = \Pr(\text{scenario} \mid \text{non-match})$ — probability among non-matches

The **Bayes Factor** $K$ is the ratio:

$$K = \frac{m}{u}$$

The **partial match weight** $\omega$ is the log₂ transform:

$$\omega = \log_2(K) = \log_2\left(\frac{m}{u}\right)$$

### Combining Evidence

**Multiplicative (current approach):**
$$\text{posterior odds} = \text{prior odds} \times K_1 \times K_2 \times \ldots \times K_n$$

Then take log₂ to get match weight:
$$\text{match weight} = \log_2(\text{prior odds}) + \log_2(K_1) + \log_2(K_2) + \ldots + \log_2(K_n)$$

**Additive (proposed approach):**
$$\text{match weight} = \omega_{\text{prior}} + \omega_1 + \omega_2 + \ldots + \omega_n$$

Where $\omega_i = \log_2(K_i)$ and $\omega_{\text{prior}} = \log_2(\text{prior odds})$.

**These are mathematically equivalent**, but the additive formulation computes directly in log-space.

---

## The Problem with Multiplicative Bayes Factors

### Current SQL Pattern

From `__splink__df_predict`:

```sql
-- Step 1: Compute Bayes Factors per comparison
CASE
  WHEN gamma_first_name = -1 THEN cast(1.0 as float8)      -- BF = 1 (neutral)
  WHEN gamma_first_name = 1  THEN cast(1024.0 as float8)   -- BF = 2^10
  WHEN gamma_first_name = 0  THEN cast(0.03125 as float8)  -- BF = 2^-5
END as bf_first_name

-- Step 2: Multiply all BFs together, then take log
log2(
  least(
    greatest(
      cast(0.00010001 as float8) * bf_first_name * bf_surname * bf_dob * bf_city * bf_email,
      1e-300
    ),
    1e300
  )
) as match_weight
```

### Failure Mode 1: Underflow → `log2(0)`

When multiple small Bayes Factors are multiplied:
- Each non-match on a comparison might give BF ≈ 0.03125 (= 2⁻⁵)
- With 10 comparisons all non-matching: $0.03125^{10} \approx 9 \times 10^{-16}$
- Add the prior odds (≈ 0.0001) and you get values approaching floating-point underflow
- When the product underflows to exactly 0, `log2(0)` throws **"can't take logarithm of zero"**

### Failure Mode 2: Extreme m/u Ratios

If $m \approx 0$ (never observe this outcome among matches) or $u \approx 0$ (never among non-matches):
- BF = 0 or BF = ∞
- Multiplication propagates these extreme values
- Current workaround requires complex `CASE WHEN ... infinity` checks

### Current Workarounds Are Fragile

```sql
-- Clamping to avoid log(0) and log(∞)
log2(least(greatest(product, 1e-300), 1e300))

-- Special handling for infinity
CASE WHEN bf_first_name = cast('infinity' as float8)
     OR bf_surname = cast('infinity' as float8)
     ...
THEN 1.0
ELSE (product)/(1+product)
END as match_probability
```

This is brittle, dialect-dependent, and doesn't address the root cause.

---

## The Solution: Compute in Log-Space

### Core Insight

Addition in log-space is inherently stable. There is no multiplication of potentially tiny or huge numbers.

$$\text{match weight} = \omega_{\text{prior}} + \sum_i \omega_i$$

Where each $\omega_i$ is a bounded value (typically in range [-30, +30]).

### Proposed SQL Pattern

```sql
-- Step 1: Compute partial match weights directly (not Bayes Factors)
CASE
  WHEN gamma_first_name = -1 THEN 0.0                      -- log2(1) = 0
  WHEN gamma_first_name = 1  THEN 10.0                     -- log2(1024) = 10
  WHEN gamma_first_name = 0  THEN -5.0                     -- log2(0.03125) = -5
END as mw_first_name

-- Step 2: Sum the partial weights (simple addition!)
(
  cast(-13.2877 as float8)  -- log2(prior_odds) ≈ log2(0.0001)
  + mw_first_name
  + mw_surname
  + mw_dob
  + mw_city
  + mw_email
) as match_weight
```

### Verification: Results Are Identical

From the current output, for row 1:
- `bf_first_name = 1024.0` → `mw_first_name = log2(1024) = 10`
- `bf_surname = 1.0` (NULL) → `mw_surname = log2(1) = 0`
- `bf_dob = 0.03125` → `mw_dob = log2(0.03125) = -5`
- `bf_city = 1.0` (NULL) → `mw_city = 0`
- `bf_tf_adj_city = 1.0` → `mw_tf_adj_city = 0`
- `bf_email = 1024.0` → `mw_email = 10`
- `prior_odds = 0.00010001...` → `mw_prior = log2(0.0001) ≈ -13.2877`

**Sum:** `10 + 0 + (-5) + 0 + 0 + 10 + (-13.2877) = 1.7123`

**Current output:** `match_weight = 1.7124318971685955` ✓

---

## Implementation Details

### Term Frequency Adjustments

**Current (multiplicative):**
```sql
CASE WHEN gamma_city = 1 THEN
  POW(base_u / max(tf_city_l, tf_city_r), 1.0)
ELSE 1.0 END AS bf_tf_adj_city
```

**Proposed (additive) — with division-by-zero protection:**
```sql
CASE WHEN gamma_city = 1 THEN
  -- Use NULLIF to convert 0 to NULL, then COALESCE to clamp
  -- This prevents division by zero AND log(0)
  LOG2(
    base_u / GREATEST(
      COALESCE(NULLIF(max(tf_city_l, tf_city_r), 0), 1e-10),
      1e-10
    )
  )
ELSE 0.0 END AS mw_tf_adj_city
```

**⚠️ Edge Case:** If term frequency lookup returns 0 or NULL, naive `LOG2(base_u / tf)` causes division-by-zero. The `GREATEST(..., 1e-10)` clamp ensures a safe minimum divisor.

### Match Probability Calculation

**Current:** Complex formula with infinity checks

**Proposed:** Stable sigmoid from log-odds
```sql
-- Convert base-2 log-odds to probability
-- Using numerically stable sigmoid
CASE
  WHEN match_weight >= 0 THEN
    1.0 / (1.0 + POW(2, -match_weight))
  ELSE
    POW(2, match_weight) / (1.0 + POW(2, match_weight))
END AS match_probability
```

### Clamping Strategy

**Three places require clamping to avoid `log(0)` or `ValueError`:**

#### 1. Prior Probability (Python-side)

```python
# In _combine_prior_and_mws() - clamp probability before log
PROB_CLAMP_MIN = 1e-10
PROB_CLAMP_MAX = 1 - 1e-10

p = probability_two_random_records_match
p = max(PROB_CLAMP_MIN, min(p, PROB_CLAMP_MAX))  # Clamp to [1e-10, 1-1e-10]

prior_odds = p / (1 - p)
mw_prior = math.log2(prior_odds)
```

**⚠️ Edge Case:** If `probability_two_random_records_match = 0.0` or `1.0` (unlikely but possible via manual settings), `math.log2` throws `ValueError: math domain error`.

#### 2. m and u Probabilities (Python-side)

```python
# When computing mw from m and u
M_U_CLAMP_MIN = 1e-10
MW_CLAMP_MAX = 100  # Corresponds to BF of ~10^30

m = max(m, M_U_CLAMP_MIN)  # prevent log(0) when m=0
u = max(u, M_U_CLAMP_MIN)  # prevent division by zero when u=0

mw = math.log2(m / u)
mw = max(min(mw, MW_CLAMP_MAX), -MW_CLAMP_MAX)  # clamp to [-100, +100]
```

#### 3. Term Frequency (SQL-side)

```sql
-- Ensure tf is never 0 to prevent division by zero
GREATEST(COALESCE(tf_value, 1e-10), 1e-10)
```

**Summary of clamping constants:**

| Constant | Value | Purpose |
|----------|-------|---------|
| `PROB_CLAMP_MIN` | `1e-10` | Minimum probability (prevents log(0) for prior) |
| `PROB_CLAMP_MAX` | `1 - 1e-10` | Maximum probability (prevents log(inf) for prior) |
| `M_U_CLAMP_MIN` | `1e-10` | Minimum m/u value (prevents log(0) and div/0) |
| `TF_CLAMP_MIN` | `1e-10` | Minimum term frequency (prevents div/0 in SQL) |
| `MW_CLAMP_MAX` | `100` | Maximum match weight magnitude (~10^30 BF) |

---

## Output Schema Changes

### Current Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `match_weight` | double | Overall log₂(posterior odds) |
| `match_probability` | double | Sigmoid of match_weight |
| `gamma_*` | int | Comparison level indicators |
| `bf_*` | double | Per-comparison Bayes Factors |
| `bf_tf_adj_*` | double | TF adjustment multipliers |
| `tf_*` | double | Term frequency values |

### Proposed Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `match_weight` | double | Overall log₂(posterior odds) — **unchanged** |
| `match_probability` | double | Sigmoid of match_weight — **unchanged** |
| `gamma_*` | int | Comparison level indicators — **unchanged** |
| `mw_*` | double | Per-comparison partial match weights — **replaces `bf_*`** |
| `mw_tf_adj_*` | double | TF adjustment (additive) — **replaces `bf_tf_adj_*`** |
| `tf_*` | double | Term frequency values — **unchanged** |

### Example Output

**Current:**
```
bf_first_name | bf_surname | bf_dob  | bf_city | bf_email | match_weight
1024.0        | 1024.0     | 0.03125 | 1.0     | 1024.0   | 11.71
```

**Proposed:**
```
mw_first_name | mw_surname | mw_dob | mw_city | mw_email | match_weight
10.0          | 10.0       | -5.0   | 0.0     | 10.0     | 11.71
```

---

## Settings Changes

### Current Settings
```python
{
    'bayes_factor_column_prefix': 'bf_',
    ...
}
```

### Proposed Settings
```python
{
    'match_weight_column_prefix': 'mw_',  # New default
    # Deprecated: 'bayes_factor_column_prefix'
    ...
}
```

---

## Benefits Summary

| Aspect | Multiplicative BFs (Current) | Additive MWs (Proposed) |
|--------|------------------------------|-------------------------|
| **log(0) risk** | Yes — products can underflow | **No** — addition is stable |
| **Overflow risk** | Yes — products can overflow | **No** — sums are bounded |
| **SQL complexity** | Complex clamping & infinity checks | Simple addition |
| **Interpretability** | BFs range 10⁻³⁰ to 10³⁰ | MWs range -100 to +100 |
| **Chart alignment** | UI shows MWs, SQL uses BFs | Both use MWs |
| **Debugging** | Must mentally convert BF↔MW | Values directly interpretable |
| **Portability** | Dialect-specific overflow handling | Works uniformly across engines |

---

## Migration Plan

### Phase 1: Internal Change (Non-Breaking)
1. Add `use_additive_match_weights=True` flag (default: `True`)
2. Compute `mw_*` columns internally using the additive formula
3. Keep `match_weight` and `match_probability` output names unchanged
4. Output `mw_*` instead of `bf_*` when `retain_intermediate_calculation_columns=True`

### Phase 2: Deprecation
1. Add `retain_bayes_factors=True` option for users who need `bf_*` columns
2. Emit deprecation warning when `bayes_factor_column_prefix` is used
3. Update documentation to emphasize match weights

### Phase 3: Removal (Future Major Version)
1. Remove `bf_*` column support
2. Remove `bayes_factor_column_prefix` setting
3. `mw_*` becomes the only intermediate column format

---

## SQL Change Summary

### `__splink__df_match_weight_parts` CTE

**Before:**
```sql
CASE
  WHEN gamma_first_name = -1 THEN cast(1.0 as float8)
  WHEN gamma_first_name = 1  THEN cast(1024.0 as float8)
  WHEN gamma_first_name = 0  THEN cast(0.03125 as float8)
END as bf_first_name
```

**After:**
```sql
CASE
  WHEN gamma_first_name = -1 THEN cast(0.0 as float8)
  WHEN gamma_first_name = 1  THEN cast(10.0 as float8)
  WHEN gamma_first_name = 0  THEN cast(-5.0 as float8)
END as mw_first_name
```

### `__splink__df_predict` CTE

**Before:**
```sql
log2(
  least(
    greatest(
      cast(0.0001 as float8) * bf_first_name * bf_surname * bf_dob * bf_city * bf_email,
      1e-300
    ),
    1e300
  )
) as match_weight
```

**After:**
```sql
(
  cast(-13.2877 as float8)  -- mw_prior = log2(prior_odds)
  + mw_first_name
  + mw_surname
  + mw_dob
  + mw_city
  + mw_email
) as match_weight
```

---

## Conclusion

Switching to additive match weights:

1. **Eliminates** the `log(0)` bug entirely
2. **Simplifies** SQL by removing clamping and infinity handling
3. **Aligns** internal computation with how we explain and visualize models
4. **Improves** numerical stability across all SQL backends
5. **Maintains** full mathematical equivalence — results are identical

The change is architecturally clean: we're simply computing `log₂(m/u)` in Python when building the SQL, rather than computing `m/u` in SQL and taking the log afterward.

---

## Step-by-Step Implementation Plan

Each step is designed to be small, self-contained, and verifiable. Steps are ordered to enable iterative building with verification at each stage.

### Step 1: Add Clamping Constants and Safe Match Weight Property (Foundation)

**File:** `splink/internals/comparison_level.py`

**Changes:**
- Add module-level constants:
  ```python
  M_U_CLAMP_MIN = 1e-10      # Minimum m/u value (prevents log(0) and div/0)
  MW_CLAMP_MAX = 100         # Maximum match weight magnitude (~10^30 BF)
  ```
- Add a new property `_match_weight` that includes clamping logic:
  ```python
  @property
  def _match_weight(self) -> float:
      if self.is_null_level:
          return 0.0
      m = max(self.m_probability or M_U_CLAMP_MIN, M_U_CLAMP_MIN)
      u = max(self.u_probability or M_U_CLAMP_MIN, M_U_CLAMP_MIN)
      mw = math.log2(m / u)
      return max(min(mw, MW_CLAMP_MAX), -MW_CLAMP_MAX)
  ```

**Verification:**
- Unit test that `_match_weight` returns clamped values for edge cases:
  - `m=0, u=0.5` → returns `-MW_CLAMP_MAX` (not error)
  - `m=0.5, u=0` → returns `+MW_CLAMP_MAX` (not error)
  - `m=None, u=None` → returns clamped default (not error)
- Run `uv run pytest tests/test_comparison_level.py -v`

---

### Step 2: Add `_match_weight_sql` Method to `ComparisonLevel`

**File:** `splink/internals/comparison_level.py`

**Changes:**
- Add new method `_match_weight_sql(self, gamma_column_name: str) -> str` that generates SQL outputting the partial match weight directly (not Bayes Factor)
- Keep existing `_bayes_factor_sql` unchanged for backward compatibility
- The new method should output `cast(X.XX as float8)` where X.XX is the pre-computed (and clamped) `_match_weight`

**Verification:**
- Unit test that SQL generated matches expected format
- Verify output values match `_match_weight` property (not `_log2_bayes_factor`)
- Run `uv run mypy splink/internals/comparison_level.py`

---

### Step 3: Add `_tf_adjustment_match_weight_sql` Method (with Division-by-Zero Protection)

**File:** `splink/internals/comparison_level.py`

**Changes:**
- Add constant: `TF_CLAMP_MIN = 1e-10`
- Add new method `_tf_adjustment_match_weight_sql(...)` that generates SQL for TF adjustment in log-space
- **Critical:** Protect against division by zero in SQL:
  ```sql
  -- Instead of: LOG2(base_u / tf)
  -- Use: LOG2(base_u / GREATEST(COALESCE(tf, 1e-10), 1e-10))
  ```
- Mathematical transform: `log2(POW(x, w)) = w * log2(x)`

**Verification:**
- Unit test with tf=0 input → must NOT error, should return clamped value
- Unit test with tf=NULL input → must NOT error
- Verify mathematical equivalence to existing TF adjustment logic
- Run `uv run mypy splink/internals/comparison_level.py`

---

### Step 4: Add Column Name Properties for Match Weights

**File:** `splink/internals/comparison.py`

**Changes:**
- Add property `_mw_column_name` → returns `mw_{output_column_name}`
- Add property `_mw_tf_adj_column_name` → returns `mw_tf_adj_{output_column_name}`
- Add property `_match_weight_columns_to_sum` (analogous to `_match_weight_columns_to_multiply`)

**Verification:**
- Unit test that column names are generated correctly
- Run `uv run mypy splink/internals/comparison.py`

---

### Step 5: Add `_columns_to_select_for_match_weight_parts` Method

**File:** `splink/internals/comparison.py`

**Changes:**
- Add new method parallel to `_columns_to_select_for_bayes_factor_parts`
- Uses `_match_weight_sql` instead of `_bayes_factor_sql`
- Uses `_tf_adjustment_match_weight_sql` instead of `_tf_adjustment_sql`
- Outputs `mw_*` columns instead of `bf_*` columns

**Verification:**
- Unit test that method generates expected SQL fragments
- Run `uv run mypy splink/internals/comparison.py`

---

### Step 6: Add `columns_to_select_for_match_weight_parts` to Settings

**File:** `splink/internals/settings.py`

**Changes:**
- Add new static method `columns_to_select_for_match_weight_parts` parallel to `columns_to_select_for_bayes_factor_parts`
- Calls the new comparison method from Step 5

**Verification:**
- Unit test that aggregates columns correctly from multiple comparisons
- Run `uv run mypy splink/internals/settings.py`

---

### Step 7: Add `_combine_prior_and_mws` Function (with Prior Clamping)

**File:** `splink/internals/predict.py`

**Changes:**
- Add constants:
  ```python
  PROB_CLAMP_MIN = 1e-10
  PROB_CLAMP_MAX = 1 - 1e-10
  ```
- Add new function `_combine_prior_and_mws(prior, mw_terms, sql_dialect)` that:
  - **Critical:** Clamps prior probability before log:
    ```python
    p = max(PROB_CLAMP_MIN, min(prior, PROB_CLAMP_MAX))
    prior_odds = p / (1 - p)
    mw_prior = math.log2(prior_odds)
    ```
  - Returns SQL expression: `(mw_prior + mw_term1 + mw_term2 + ...)`
  - Returns match probability expression using stable sigmoid

**Verification:**
- Unit test with `prior=0.0` → must NOT raise `ValueError`, should use clamped value
- Unit test with `prior=1.0` → must NOT raise `ValueError`, should use clamped value
- Unit test that SQL expression is mathematically correct for normal priors
- Verify against hand-calculated examples
- Run `uv run mypy splink/internals/predict.py`

---

### Step 8: Add `predict_from_comparison_vectors_sqls_additive` Function

**File:** `splink/internals/predict.py`

**Changes:**
- Add new function that generates predict SQL using additive match weights
- Uses `columns_to_select_for_match_weight_parts` from Step 6
- Uses `_combine_prior_and_mws` from Step 7
- Keep original function unchanged

**Verification:**
- Integration test: run both functions on same data, verify `match_weight` and `match_probability` are identical
- Run `uv run pytest -m duckdb -k predict`

---

### Step 9: Add `use_additive_match_weights` Flag to Settings

**Files:**
- `splink/internals/settings.py`
- `splink/internals/settings_creator.py`

**Changes:**
- Add `use_additive_match_weights: bool = True` parameter
- Add `match_weight_column_prefix: str = "mw_"` to `ColumnInfoSettings`
- Wire through to Settings class

**Verification:**
- Test that flag is correctly passed through settings chain
- Run `uv run mypy splink/internals/settings.py splink/internals/settings_creator.py`

---

### Step 10: Route Prediction Through Additive Path

**File:** `splink/internals/predict.py`

**Changes:**
- Modify `predict_from_comparison_vectors_sqls` to check `use_additive_match_weights` flag
- If True, call the new additive function from Step 8
- If False, use existing multiplicative function

**Verification:**
- Integration test with `use_additive_match_weights=True` (default)
- Integration test with `use_additive_match_weights=False` (legacy)
- Verify both produce identical `match_weight` values
- Run `uv run pytest -m duckdb`

---

### Step 11: Update Waterfall Chart to Use Match Weights

**File:** `splink/internals/waterfall_chart.py`

**Changes:**
- Modify `_comparison_records` to read `mw_*` columns when available, falling back to `bf_*`
- The `log2_bayes_factor` field should come from `mw_*` column directly (no conversion needed)
- Handle backward compatibility for older prediction results

**Verification:**
- Visual test: generate waterfall chart with new predictions
- Verify chart displays correctly
- Run `uv run pytest -k waterfall`

---

### Step 12: Update Tests to Use New Column Names

**Files:**
- `tests/test_term_frequencies.py`
- `tests/test_settings_options.py`
- Other test files referencing `bf_*` columns

**Changes:**
- Update tests to expect `mw_*` columns instead of `bf_*`
- Add tests for backward compatibility flag

**Verification:**
- Run full test suite: `uv run pytest -m duckdb`
- Run `uv run pytest -m spark` if spark tests exist

---

### Step 13: Documentation Updates

**Files:**
- `docs/topic_guides/*.md`
- `docs/api_docs/*.md`
- Docstrings in modified files

**Changes:**
- Update documentation to explain match weights vs Bayes factors
- Add migration guide for users expecting `bf_*` columns
- Update API documentation for new settings

**Verification:**
- Build docs locally: `./scripts/make_docs_locally.sh`
- Review generated documentation

---

### Step 14: Deprecation Warnings

**Files:**
- `splink/internals/settings.py`
- `splink/internals/comparison.py`

**Changes:**
- Add deprecation warning when `bayes_factor_column_prefix` is explicitly set
- Add deprecation warning when accessing `_bf_column_name` properties

**Verification:**
- Test that warnings are emitted correctly
- Run `uv run pytest -W error::DeprecationWarning` to catch unintended warnings

---

## Key Files to Modify

The following files contain the core logic that needs to change:

### Core SQL Generation (Primary)

| File | Purpose |
|------|---------|
| `splink/internals/comparison_level.py` | Generates per-level BF/MW SQL (`_bayes_factor_sql`, `_tf_adjustment_sql`) |
| `splink/internals/comparison.py` | Aggregates level SQL into comparison SQL (`_columns_to_select_for_bayes_factor_parts`) |
| `splink/internals/predict.py` | Combines comparisons into final prediction SQL (`_combine_prior_and_bfs`) |

### Settings & Configuration

| File | Purpose |
|------|---------|
| `splink/internals/settings.py` | `ColumnInfoSettings.bayes_factor_column_prefix`, static column selection methods |
| `splink/internals/settings_creator.py` | User-facing settings dataclass |

### Visualization & Output

| File | Purpose |
|------|---------|
| `splink/internals/waterfall_chart.py` | Reads `bf_*` columns for visualization |
| `splink/internals/charts.py` | Chart generation utilities |

### Helper Functions

| File | Purpose |
|------|---------|
| `splink/internals/misc.py` | `prob_to_bayes_factor`, `prob_to_match_weight`, `match_weight_to_bayes_factor` |

### Tests

| File | Purpose |
|------|---------|
| `tests/test_term_frequencies.py` | References `bf_*` columns directly |
| `tests/test_settings_options.py` | Tests `bayes_factor_column_prefix` setting |
| `tests/basic_settings.py` | Uses `bayes_factor_to_prob`, `prob_to_bayes_factor` |

---

## File List (Relative Paths)

```
splink/internals/comparison_level.py
splink/internals/comparison.py
splink/internals/predict.py
splink/internals/settings.py
splink/internals/settings_creator.py
splink/internals/waterfall_chart.py
splink/internals/charts.py
splink/internals/misc.py
splink/internals/linker.py
splink/internals/linker_components/inference.py
splink/internals/linker_components/visualisations.py
tests/test_term_frequencies.py
tests/test_settings_options.py
tests/basic_settings.py
tests/test_predict.py (if exists)
```

---

## References

- [GitHub Issue #1889](https://github.com/moj-analytical-services/splink/issues/1889) — Original bug report
- [Fellegi-Sunter Model Documentation](https://moj-analytical-services.github.io/splink/) — m and u probabilities
- [Match Weights Tutorial](https://www.robinlinacre.com/m_and_u_probabilities/) — Relationship between BFs and MWs
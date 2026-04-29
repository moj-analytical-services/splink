"""Helpers for downsampling input records prior to EM training.

We want to limit the number of pairwise comparisons used by an EM training
session to a user-specified `max_pairs`.  The strategy is:

1. Compute a cheap Cartesian upper bound on the full pair count from the row
   counts of the input table(s).  This bounds how aggressive the probe can
   be without itself blowing up.
2. Take a hash-based probe sample of records, sized so the worst-case probe
   pair count stays under `max_probe_pairs`.  Run blocking on the probe.
3. Count the resulting blocked pairs `C0`.  Estimate the full pair count as
   `P_hat = C0 / actual_probe_fraction**2`.  This holds because pair count
   scales as the product of left- and right-side record counts, both of
   which scale linearly with the sample fraction (for any link type).
4. If the probe pair count is too small to give a stable estimate, escalate
   the probe proportion (subject to the same cap) and retry.
5. Solve `p* = sqrt(max_pairs / P_hat)` and return `(sample_threshold,
   sample_modulus)` describing a hash predicate that retains approximately
   fraction `p*` of input records.

Two important details:

- The probe and the final sample use *different* hash salts, so they are
  statistically independent.
- We always use the *actual* fraction implied by the integer threshold
  (threshold / modulus) when extrapolating the probe pair count, never the
  user-supplied float `probe_proportion`.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any

from splink.internals.blocking import block_using_rules_sqls
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import enqueue_df_concat

if TYPE_CHECKING:
    from splink.internals.blocking import BlockingRule
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


# Modulus used for the probe sampling hash filter.  Small modulus is fine
# because we only need ~2dp resolution on the probe fraction.
_PROBE_SAMPLE_MODULUS = 10_000

# Modulus used for the final sampling hash filter.  We want enough resolution
# that the integer threshold can closely approximate any p* in (0, 1].
_SAMPLE_MODULUS = 1_000_000

# Default minimum probe blocked-pair count required to consider the
# extrapolation reliable.  Below this we escalate the probe proportion.
_DEFAULT_MIN_PROBE_PAIRS = 1_000

# Default maximum pair count we are willing to materialise during the probe
# itself.  Without this, a 1% probe of a dataset whose blocking rule would
# produce ~1e12 pairs would still produce ~1e8 pairs.
_DEFAULT_MAX_PROBE_PAIRS = 1_000_000.0


def _em_hash_salt(seed: int | None, purpose: str) -> str:
    """Compose the salt string used for the probe / final hash filters.

    Using different `purpose` strings for "probe" and "sample" makes the two
    filters statistically independent.
    """
    seed_part = "default" if seed is None else str(seed)
    return f"__splink_em_{purpose}_{seed_part}__"


def _probe_sample_threshold(probe_proportion: float) -> int:
    """Convert a probe proportion (e.g. 0.01) into an integer threshold to use
    against `_PROBE_SAMPLE_MODULUS`."""
    if not 0 < probe_proportion <= 1:
        raise ValueError(
            f"probe_proportion must be in (0, 1]; got {probe_proportion!r}"
        )
    return min(
        _PROBE_SAMPLE_MODULUS,
        max(1, math.ceil(probe_proportion * _PROBE_SAMPLE_MODULUS)),
    )


def _probe_actual_fraction(sample_threshold: int) -> float:
    return sample_threshold / _PROBE_SAMPLE_MODULUS


def _cartesian_upper_bound_for_link_type(
    *,
    linker: Linker,
) -> int:
    """Upper bound on the number of pairs the unsampled blocking rule could
    possibly produce, ignoring the rule's selectivity.

    Used to size the probe safely; it is *not* used as the pair-count
    estimate (which always comes from the probe).
    """
    settings = linker._settings_obj
    db_api = linker._db_api
    link_type = settings._link_type
    sds_col = settings.column_info_settings.source_dataset_input_column

    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)

    if link_type == "link_only" and sds_col is not None:
        pipeline.enqueue_sql(
            f"select {sds_col.name} as sds, count(*) as c "
            "from __splink__df_concat group by 1",
            "__splink__em_row_counts",
        )
        df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        rows = df.as_record_dict()
        df.drop_table_from_database_and_remove_from_cache()
        sizes = [int(r["c"]) for r in rows]
        total = sum(sizes)
        sum_sq = sum(s * s for s in sizes)
        # Cross-source pairs only:  (T^2 - sum_i n_i^2) / 2
        upper = max(0, (total * total - sum_sq) // 2)
    else:
        pipeline.enqueue_sql(
            "select count(*) as c from __splink__df_concat",
            "__splink__em_row_count",
        )
        df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        rows = df.as_record_dict()
        df.drop_table_from_database_and_remove_from_cache()
        n = int(rows[0]["c"]) if rows else 0
        upper = (n * (n - 1)) // 2

    logger.info(
        "[EM sampling] Cartesian pair-count upper bound for link_type=%s: %d",
        link_type,
        upper,
    )
    return upper


def _safe_probe_proportion(
    *,
    requested: float,
    cartesian_upper_bound: int,
    max_probe_pairs: float,
) -> float:
    """Cap `requested` so the worst-case probe pair count stays under
    `max_probe_pairs`.

    Worst-case probe pairs ~= cartesian_upper_bound * p^2, so we need
    p <= sqrt(max_probe_pairs / cartesian_upper_bound).
    """
    if cartesian_upper_bound <= 0:
        return requested
    p_cap = math.sqrt(max_probe_pairs / cartesian_upper_bound)
    p_cap = min(1.0, p_cap)
    capped = min(requested, p_cap)
    # We always need at least one row; never reduce below 1/PROBE_MODULUS.
    return max(capped, 1.0 / _PROBE_SAMPLE_MODULUS)


def _count_blocked_pairs_for_probe(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    probe_proportion: float,
    sample_salt: str,
) -> tuple[int, float]:
    """Run blocking on a probe-sampled record set.

    Returns `(blocked_pair_count, actual_probe_fraction)`.
    """
    settings = linker._settings_obj
    db_api = linker._db_api

    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)

    sample_threshold = _probe_sample_threshold(probe_proportion)
    actual_fraction = _probe_actual_fraction(sample_threshold)

    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=[blocking_rule],
        link_type=settings._link_type,
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=(settings.column_info_settings.unique_id_input_column),
        sample_threshold=sample_threshold,
        sample_modulus=_PROBE_SAMPLE_MODULUS,
        sample_salt=sample_salt,
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pipeline.enqueue_sql(
        "select count(*) as row_count from __splink__blocked_id_pairs",
        "__splink__em_probe_count",
    )

    df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    rows = df.as_record_dict()
    df.drop_table_from_database_and_remove_from_cache()
    count = int(rows[0]["row_count"]) if rows else 0

    logger.info(
        "[EM sampling] Probe at proportion %.6f (actual fraction %.6f, "
        "sample_threshold=%d / sample_modulus=%d) -> %d blocked pairs",
        probe_proportion,
        actual_fraction,
        sample_threshold,
        _PROBE_SAMPLE_MODULUS,
        count,
    )
    return count, actual_fraction


def resolve_em_sample_threshold(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    max_pairs: float | None,
    probe_proportion: float,
    seed: int | None = None,
    max_probe_pairs: float = _DEFAULT_MAX_PROBE_PAIRS,
    min_probe_pairs_for_calibration: int = _DEFAULT_MIN_PROBE_PAIRS,
) -> tuple[int | None, int, dict[str, Any]]:
    """Decide whether (and how) to downsample input records for EM training.

    Returns `(sample_threshold, sample_modulus, info)`.  A `sample_threshold`
    of None indicates that no sampling should be applied.  `info` is a dict
    of intermediate values useful for logging / tests.
    """
    if max_pairs is not None and max_pairs <= 0:
        raise ValueError(f"max_pairs must be positive, or None; got {max_pairs!r}")
    if not 0 < probe_proportion <= 1:
        raise ValueError(
            f"probe_proportion must be in (0, 1]; got {probe_proportion!r}"
        )
    if max_probe_pairs <= 0:
        raise ValueError(f"max_probe_pairs must be positive; got {max_probe_pairs!r}")
    if min_probe_pairs_for_calibration <= 0:
        raise ValueError(
            "min_probe_pairs_for_calibration must be positive; "
            f"got {min_probe_pairs_for_calibration!r}"
        )

    probe_salt = _em_hash_salt(seed, "probe")
    sample_salt = _em_hash_salt(seed, "sample")

    info: dict[str, Any] = {
        "max_pairs": max_pairs,
        "max_probe_pairs": max_probe_pairs,
        "min_probe_pairs_for_calibration": min_probe_pairs_for_calibration,
        "cartesian_upper_bound": None,
        "requested_probe_proportion": probe_proportion,
        "probe_proportion_used": None,
        "actual_probe_fraction": None,
        "probe_pair_count": None,
        "estimated_total_pairs": None,
        "p_star": None,
        "sample_threshold": None,
        "sample_modulus": _SAMPLE_MODULUS,
        "expected_pairs_after_sampling": None,
        "sampling_applied": False,
        "probe_salt": probe_salt,
        "sample_salt": sample_salt,
        "reason": None,
    }

    if max_pairs is None:
        info["reason"] = "max_pairs is None"
        logger.info("[EM sampling] max_pairs is None — no sampling will be applied")
        return None, _SAMPLE_MODULUS, info

    cartesian = _cartesian_upper_bound_for_link_type(linker=linker)
    info["cartesian_upper_bound"] = cartesian

    capped_probe = _safe_probe_proportion(
        requested=probe_proportion,
        cartesian_upper_bound=cartesian,
        max_probe_pairs=max_probe_pairs,
    )
    if capped_probe < probe_proportion:
        logger.info(
            "[EM sampling] Capping requested probe_proportion %.6f -> %.6f "
            "to keep worst-case probe pair count below max_probe_pairs=%.0f "
            "(cartesian upper bound = %d)",
            probe_proportion,
            capped_probe,
            max_probe_pairs,
            cartesian,
        )

    # Probe escalation ladder: start at the (capped) requested proportion;
    # if probe pair count is below `min_probe_pairs_for_calibration`,
    # escalate up to 1.0 — but never above the safety cap.
    cap = _safe_probe_proportion(
        requested=1.0,
        cartesian_upper_bound=cartesian,
        max_probe_pairs=max_probe_pairs,
    )
    candidates = [capped_probe]
    for escalation in (capped_probe * 10, capped_probe * 100, cap):
        next_p = min(escalation, cap)
        if next_p > candidates[-1] + 1e-9:
            candidates.append(next_p)

    probe_count = 0
    actual_fraction = 0.0
    used_probe_proportion = capped_probe

    for p in candidates:
        used_probe_proportion = p
        probe_count, actual_fraction = _count_blocked_pairs_for_probe(
            linker=linker,
            blocking_rule=blocking_rule,
            probe_proportion=p,
            sample_salt=probe_salt,
        )
        if probe_count >= min_probe_pairs_for_calibration:
            break
        if probe_count > 0 and p >= cap - 1e-9:
            logger.warning(
                "[EM sampling] Probe at maximum safe proportion %.6f only "
                "returned %d pairs (< min_probe_pairs_for_calibration=%d); "
                "proceeding with a noisy estimate.",
                p,
                probe_count,
                min_probe_pairs_for_calibration,
            )
            break

    info["probe_proportion_used"] = used_probe_proportion
    info["probe_pair_count"] = probe_count
    info["actual_probe_fraction"] = actual_fraction

    if probe_count == 0:
        info["reason"] = (
            "Probe returned zero blocked pairs even at the maximum safe "
            "probe proportion; skipping sampling"
        )
        logger.warning("[EM sampling] %s", info["reason"])
        return None, _SAMPLE_MODULUS, info

    p_hat = probe_count / (actual_fraction**2)
    info["estimated_total_pairs"] = p_hat
    logger.info(
        "[EM sampling] Estimated total blocked pairs (no sampling) = "
        "%.0f (from %d probe pairs at actual fraction %.6f)",
        p_hat,
        probe_count,
        actual_fraction,
    )

    if p_hat <= max_pairs:
        info["reason"] = (
            f"Estimated total pairs ({p_hat:.0f}) is already <= max_pairs "
            f"({max_pairs:.0f}); no sampling applied"
        )
        logger.info("[EM sampling] %s", info["reason"])
        return None, _SAMPLE_MODULUS, info

    p_star = math.sqrt(max_pairs / p_hat)
    p_star = min(max(p_star, 0.0), 1.0)
    sample_threshold = max(1, int(round(p_star * _SAMPLE_MODULUS)))
    actual_p = sample_threshold / _SAMPLE_MODULUS
    expected_pairs = p_hat * actual_p**2

    info["p_star"] = p_star
    info["sample_threshold"] = sample_threshold
    info["expected_pairs_after_sampling"] = expected_pairs
    info["sampling_applied"] = True
    info["reason"] = "max_pairs cap requires downsampling"

    logger.info(
        "[EM sampling] Chose sampling fraction p*=%.6f -> "
        "sample_threshold=%d / sample_modulus=%d "
        "(actual fraction %.6f, expected blocked pairs after sampling: "
        "%.0f, target max_pairs: %.0f)",
        p_star,
        sample_threshold,
        _SAMPLE_MODULUS,
        actual_p,
        expected_pairs,
        max_pairs,
    )
    return sample_threshold, _SAMPLE_MODULUS, info

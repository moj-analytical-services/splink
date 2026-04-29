"""Helpers for downsampling input records prior to EM training.

We want to limit the number of pairwise comparisons used by an EM training
session to a user-specified `max_pairs`.  The strategy is:

1. Take a small probe sample of records (default 1%) using a deterministic
   hash filter on the unique id, then run blocking on it.
2. Count the resulting blocked pairs `C0`.  Estimate the full pair count as
   `P_hat = C0 / probe_proportion ** 2` (since pair count scales as the
   product of left- and right-side record counts, both of which scale linearly
   with the sample fraction).
3. Solve for the sampling fraction `p* = sqrt(max_pairs / P_hat)`.
4. Return `(sample_threshold, sample_modulus)` describing a hash predicate
   that retains approximately fraction `p*` of input records.
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


# Modulus used for the probe sampling hash filter.  Small modulus is fine here
# because we only need ~2dp resolution on the probe fraction.
_PROBE_SAMPLE_MODULUS = 10_000

# Modulus used for the final sampling hash filter.  We want enough resolution
# that the integer threshold can closely approximate any p* in (0, 1].
_SAMPLE_MODULUS = 1_000_000

# Default probe proportions to try in order if earlier probes return zero
# blocked pairs (small inputs / very selective blocking rules).
_PROBE_FALLBACKS = (0.01, 0.1, 1.0)


def _probe_sample_threshold(probe_proportion: float) -> int:
    """Convert a probe proportion (e.g. 0.01) into an integer threshold to use
    against `_PROBE_SAMPLE_MODULUS`."""
    return max(1, int(round(probe_proportion * _PROBE_SAMPLE_MODULUS)))


def _count_blocked_pairs_for_probe(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    probe_proportion: float,
    sample_salt: str,
) -> int:
    """Run blocking on a probe-sampled record set and return the number of
    blocked pairs produced."""

    settings = linker._settings_obj
    db_api = linker._db_api

    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)

    sample_threshold = _probe_sample_threshold(probe_proportion)

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
        "[EM sampling] Probe at proportion %.4f -> "
        "sample_threshold=%d / sample_modulus=%d -> %d blocked pairs",
        probe_proportion,
        sample_threshold,
        _PROBE_SAMPLE_MODULUS,
        count,
    )
    return count


def resolve_em_sample_threshold(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    max_pairs: float | None,
    probe_proportion: float,
    sample_salt: str,
) -> tuple[int | None, int, dict[str, Any]]:
    """Decide whether (and how) to downsample input records for EM training.

    Returns:
        A tuple `(sample_threshold, sample_modulus, info)`:
            - sample_threshold: integer threshold for the hash filter, or
              None to indicate no sampling should be applied.
            - sample_modulus: integer modulus for the hash filter
              (always `_SAMPLE_MODULUS`).
            - info: dict of values used in the calculation, useful for
              logging or testing.
    """
    info: dict[str, Any] = {
        "max_pairs": max_pairs,
        "probe_proportion_used": None,
        "probe_pair_count": None,
        "estimated_total_pairs": None,
        "p_star": None,
        "sample_threshold": None,
        "sample_modulus": _SAMPLE_MODULUS,
        "expected_pairs_after_sampling": None,
        "sampling_applied": False,
        "reason": None,
    }

    if max_pairs is None:
        info["reason"] = "max_pairs is None"
        logger.info("[EM sampling] max_pairs is None — no sampling will be applied")
        return None, _SAMPLE_MODULUS, info

    # Try the requested probe_proportion first, then fall back to larger
    # probes if we observed zero pairs (very selective rules / tiny inputs).
    probe_sequence = [probe_proportion]
    for fallback in _PROBE_FALLBACKS:
        if fallback > probe_sequence[-1]:
            probe_sequence.append(fallback)

    probe_count = 0
    used_probe_proportion = probe_proportion
    for p in probe_sequence:
        used_probe_proportion = p
        probe_count = _count_blocked_pairs_for_probe(
            linker=linker,
            blocking_rule=blocking_rule,
            probe_proportion=p,
            sample_salt=sample_salt,
        )
        if probe_count > 0:
            break

    info["probe_proportion_used"] = used_probe_proportion
    info["probe_pair_count"] = probe_count

    if probe_count == 0:
        info["reason"] = (
            "Probe returned zero blocked pairs even at probe_proportion=1.0; "
            "skipping sampling"
        )
        logger.warning("[EM sampling] %s", info["reason"])
        return None, _SAMPLE_MODULUS, info

    p_hat = probe_count / (used_probe_proportion**2)
    info["estimated_total_pairs"] = p_hat
    logger.info(
        "[EM sampling] Estimated total blocked pairs (no sampling) = "
        "%.0f (from %d probe pairs at proportion %.4f)",
        p_hat,
        probe_count,
        used_probe_proportion,
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
    expected_pairs = p_hat * (sample_threshold / _SAMPLE_MODULUS) ** 2

    info["p_star"] = p_star
    info["sample_threshold"] = sample_threshold
    info["expected_pairs_after_sampling"] = expected_pairs
    info["sampling_applied"] = True
    info["reason"] = "max_pairs cap requires downsampling"

    logger.info(
        "[EM sampling] Chose sampling fraction p*=%.6f -> "
        "sample_threshold=%d / sample_modulus=%d "
        "(expected blocked pairs after sampling: %.0f, target max_pairs: %.0f)",
        p_star,
        sample_threshold,
        _SAMPLE_MODULUS,
        expected_pairs,
        max_pairs,
    )
    return sample_threshold, _SAMPLE_MODULUS, info

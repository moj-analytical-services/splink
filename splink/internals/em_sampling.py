from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any

from splink.internals.input_column import InputColumn
from splink.internals.pipeline import CTEPipeline
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql
from splink.internals.vertically_concatenate import enqueue_df_concat

if TYPE_CHECKING:
    from splink.internals.blocking import BlockingRule
    from splink.internals.dialects import SplinkDialect
    from splink.internals.linker import Linker

logger = logging.getLogger(__name__)


# Modulus used for the probe sampling hash filter.  Small modulus is fine
# because we only need ~2dp resolution on the probe fraction.
_PROBE_SAMPLE_MODULUS = 10_000

# Modulus used for the final sampling hash filter.  We want enough resolution
# that the integer threshold can closely approximate any p* in (0, 1].
_SAMPLE_MODULUS = 1_000_000


def _em_sample_filter_sql(
    unique_id_cols: list[InputColumn],
    sample_threshold: int,
    sample_modulus: int,
    table_prefix: str,
    dialect: "SplinkDialect",
) -> str:
    """Generate a SQL WHERE clause condition for EM record sampling.

    The sample is deterministic and uses a hash of the composite unique ID.

    Args:
        unique_id_cols: The columns that form the unique ID.
        sample_threshold: Integer in [0, sample_modulus]. A row is retained
            iff hash_bucket(composite_uid, sample_modulus) < sample_threshold.
        sample_modulus: Integer giving the resolution of the sampling fraction.
        table_prefix: Table alias prefix (e.g. 'l' or 'r').
        dialect: SQL dialect for the hash function.

    Returns:
        SQL WHERE clause condition like: " AND hash(... ) % 100 < 10".
    """
    if sample_threshold >= sample_modulus:
        return ""

    if sample_threshold <= 0:
        return " AND 1=0"

    composite_id = _composite_unique_id_from_nodes_sql(unique_id_cols, table_prefix)
    sample_bucket = dialect.hash_bucket_expression(composite_id, sample_modulus)
    return f" AND {sample_bucket} < {sample_threshold}"


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


def _estimate_total_pairs(probe_count: int, actual_fraction: float) -> float:
    return probe_count / (actual_fraction**2)


def _count_blocked_pairs_for_probe(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    probe_proportion: float,
) -> tuple[int, float]:
    """Run blocking on a probe-sampled record set.

    Returns `(blocked_pair_count, actual_sample_fraction)`.
    """
    from splink.internals.blocking import block_using_rules_sqls

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
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pipeline.enqueue_sql(
        "select count(*) as row_count from __splink__blocked_id_pairs",
        "__splink__em_probe_count",
    )

    df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    rows = df.as_record_list()
    df.drop_table_from_database_and_remove_from_cache()
    count = int(rows[0]["row_count"]) if rows else 0

    logger.info(
        "[EM sampling] Probe at proportion %.6f (actual fraction %.6f, "
        "sample_threshold=%s / sample_modulus=%s) -> %s blocked pairs",
        probe_proportion,
        actual_fraction,
        f"{sample_threshold:,}",
        f"{_PROBE_SAMPLE_MODULUS:,}",
        f"{count:,}",
    )
    return count, actual_fraction


def resolve_em_sample_threshold(
    *,
    linker: Linker,
    blocking_rule: "BlockingRule",
    max_pairs: float | None,
    probe_proportion: float,
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
            "record_sample_proportion must be in (0, 1]; got " f"{probe_proportion!r}"
        )

    info: dict[str, Any] = {
        "max_pairs": max_pairs,
        "requested_record_sample_proportion": probe_proportion,
        "record_sample_proportion_used": None,
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

    probe_count, actual_fraction = _count_blocked_pairs_for_probe(
        linker=linker,
        blocking_rule=blocking_rule,
        probe_proportion=probe_proportion,
    )

    info["record_sample_proportion_used"] = probe_proportion
    info["probe_pair_count"] = probe_count

    if probe_count == 0:
        info["reason"] = "Probe returned zero blocked pairs; skipping sampling"
        logger.warning("[EM sampling] %s", info["reason"])
        return None, _SAMPLE_MODULUS, info

    p_hat = _estimate_total_pairs(probe_count, actual_fraction)
    info["estimated_total_pairs"] = p_hat
    logger.info(
        "[EM sampling] Estimated total blocked pairs (no sampling) = "
        "%s (from %s probe pairs at actual fraction %.6f)",
        f"{p_hat:,.0f}",
        f"{probe_count:,}",
        actual_fraction,
    )

    if p_hat <= max_pairs:
        info["reason"] = (
            f"Estimated total pairs ({p_hat:,.0f}) is already <= max_pairs "
            f"({max_pairs:,.0f}); no sampling applied"
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
        "sample_threshold=%s / sample_modulus=%s "
        "(actual fraction %.6f, expected blocked pairs after sampling: "
        "%s, target max_pairs: %s)",
        p_star,
        f"{sample_threshold:,}",
        f"{_SAMPLE_MODULUS:,}",
        actual_p,
        f"{expected_pairs:,.0f}",
        f"{max_pairs:,.0f}",
    )
    return sample_threshold, _SAMPLE_MODULUS, info

"""Tests for `max_pairs` based pre-blocking record sampling in EM training.

These tests are duckdb-only and intentionally verbose: they exercise the
sampling helpers, the SQL hash filter, the probe-based pair-count estimation,
and the integration into `EMTrainingSession`.

The tests use the `fake_1000` dataset (1,000 records) and the
`block_on('first_name')` training rule, which generates roughly a few thousand
pairs.  Because the dataset is small, sampled pair counts are noisy — most
"approximate" assertions therefore use generous tolerances (typically ±50%).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

import splink.internals.comparison_library as cl
from splink import DuckDBAPI, SettingsCreator, block_on
from splink.internals.em_sampling import (
    _PROBE_SAMPLE_MODULUS,
    _SAMPLE_MODULUS,
    _probe_sample_threshold,
    resolve_em_sample_threshold,
)
from splink.internals.linker import Linker

pytestmark = pytest.mark.duckdb


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_1000_df() -> pd.DataFrame:
    return pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


def _basic_settings(link_type: str = "dedupe_only") -> SettingsCreator:
    return SettingsCreator(
        link_type=link_type,
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=["l.surname = r.surname"],
    )


def _make_dedupe_linker(df: pd.DataFrame) -> Linker:
    db_api = DuckDBAPI()
    sdf = db_api.register(df)
    return Linker(sdf, _basic_settings("dedupe_only"))


def _make_link_only_linker(
    df: pd.DataFrame,
) -> Linker:
    # Split fake_1000 into two halves; each becomes its own source dataset.
    df_a = df.iloc[: len(df) // 2].copy()
    df_b = df.iloc[len(df) // 2 :].copy()

    db_api = DuckDBAPI()
    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=["l.surname = r.surname"],
    )
    sdf_a = db_api.register(df_a, "df_a")
    sdf_b = db_api.register(df_b, "df_b")
    return Linker([sdf_a, sdf_b], settings)


def _make_link_and_dedupe_linker(df: pd.DataFrame) -> Linker:
    df_a = df.iloc[: len(df) // 2].copy()
    df_b = df.iloc[len(df) // 2 :].copy()
    db_api = DuckDBAPI()
    sdf_a = db_api.register(df_a, "df_a")
    sdf_b = db_api.register(df_b, "df_b")
    settings = _basic_settings("link_and_dedupe")
    return Linker([sdf_a, sdf_b], settings)


def _train_and_get_session(linker: Linker, **em_kwargs):
    return linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name"),
        **em_kwargs,
    )


# ---------------------------------------------------------------------------
# Unit tests for the helpers
# ---------------------------------------------------------------------------


def test_probe_sample_threshold_arithmetic():
    # 1% of 10,000 should be 100
    assert _probe_sample_threshold(0.01) == 100
    # 10% should be 1000
    assert _probe_sample_threshold(0.1) == 1000
    # full sample
    assert _probe_sample_threshold(1.0) == _PROBE_SAMPLE_MODULUS
    # invalid: zero or negative or above 1 must raise
    with pytest.raises(ValueError):
        _probe_sample_threshold(0.0)
    with pytest.raises(ValueError):
        _probe_sample_threshold(-0.1)
    with pytest.raises(ValueError):
        _probe_sample_threshold(1.5)


# ---------------------------------------------------------------------------
# resolve_em_sample_threshold: probe runs and returns sensible values
# ---------------------------------------------------------------------------


def test_resolve_em_sample_threshold_no_max_pairs(fake_1000_df, caplog):
    linker = _make_dedupe_linker(fake_1000_df)
    br = block_on("first_name").get_blocking_rule("duckdb")

    with caplog.at_level(logging.INFO, logger="splink.internals.em_sampling"):
        sample_threshold, sample_modulus, info = resolve_em_sample_threshold(
            linker=linker,
            blocking_rule=br,
            max_pairs=None,
            probe_proportion=0.01,
        )

    assert sample_threshold is None
    assert sample_modulus == _SAMPLE_MODULUS
    assert info["sampling_applied"] is False
    assert info["max_pairs"] is None
    # No probe should be run when max_pairs is None
    assert info["probe_pair_count"] is None


def test_resolve_em_sample_threshold_no_op_when_max_pairs_already_high(
    fake_1000_df, caplog
):
    linker = _make_dedupe_linker(fake_1000_df)
    br = block_on("first_name").get_blocking_rule("duckdb")

    with caplog.at_level(logging.INFO, logger="splink.internals.em_sampling"):
        sample_threshold, _, info = resolve_em_sample_threshold(
            linker=linker,
            blocking_rule=br,
            max_pairs=1e9,
            probe_proportion=0.1,
        )

    # Estimated pair count for fake_1000 + first_name block is in the low
    # thousands, well below 1e9, so no sampling should be applied.
    assert sample_threshold is None
    assert info["sampling_applied"] is False
    assert info["estimated_total_pairs"] is not None
    assert info["estimated_total_pairs"] < 1e9


def test_resolve_em_sample_threshold_estimates_pair_count_within_tolerance(
    fake_1000_df,
):
    """Probe-based pair-count estimate should be in the right ballpark."""
    linker = _make_dedupe_linker(fake_1000_df)
    br = block_on("first_name").get_blocking_rule("duckdb")

    # First, get the actual pair count by running blocking with no sampling.
    from splink.internals.blocking import block_using_rules_sqls
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.vertically_concatenate import enqueue_df_concat

    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)
    settings = linker._settings_obj
    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=[br],
        link_type=settings._link_type,
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=(settings.column_info_settings.unique_id_input_column),
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pipeline.enqueue_sql("select count(*) as c from __splink__blocked_id_pairs", "__c")
    df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    actual_pairs = int(df.as_record_dict()[0]["c"])
    df.drop_table_from_database_and_remove_from_cache()

    # Use a relatively large probe so that the estimate is statistically
    # stable on this small (1000-row) dataset.
    _, _, info = resolve_em_sample_threshold(
        linker=linker,
        blocking_rule=br,
        max_pairs=10,  # forces sampling, but we only care about the estimate
        probe_proportion=0.5,
    )

    estimated = info["estimated_total_pairs"]
    assert estimated is not None
    # With a 50% probe on 1000 records, allow a generous ±70% band.
    assert (
        0.3 * actual_pairs <= estimated <= 1.7 * actual_pairs
    ), f"Probe estimated {estimated:.0f} pairs vs actual {actual_pairs}"


# ---------------------------------------------------------------------------
# End-to-end: EM training applies the sampling filter and produces ~max_pairs
# ---------------------------------------------------------------------------


def _count_cvv_rows(session) -> int:
    """Re-run blocking with the same sample threshold and count rows."""
    from splink.internals.blocking import block_using_rules_sqls
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.vertically_concatenate import enqueue_df_concat

    linker = session._original_linker
    settings = linker._settings_obj
    pipeline = CTEPipeline()
    enqueue_df_concat(linker, pipeline)
    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=[session._blocking_rule_for_training],
        link_type=settings._link_type,
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=(settings.column_info_settings.unique_id_input_column),
        sample_threshold=session._sample_threshold,
        sample_modulus=session._sample_modulus,
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pipeline.enqueue_sql("select count(*) as c from __splink__blocked_id_pairs", "__c")
    df = linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)
    n = int(df.as_record_dict()[0]["c"])
    df.drop_table_from_database_and_remove_from_cache()
    return n


def test_em_max_pairs_dedupe_only_reduces_pair_count(fake_1000_df, caplog):
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    target_max_pairs = 500
    with caplog.at_level(logging.INFO, logger="splink.internals.em_sampling"):
        session = _train_and_get_session(
            linker,
            max_pairs=target_max_pairs,
            probe_proportion=0.2,  # high-ish probe for noise control
        )

    assert session._sample_threshold is not None
    assert session._sample_modulus == _SAMPLE_MODULUS
    info = session._sample_info
    assert info is not None
    assert info["sampling_applied"] is True
    assert 0 < info["p_star"] <= 1.0

    actual_count = _count_cvv_rows(session)
    expected = info["expected_pairs_after_sampling"]
    # Allow a ±70% band for the small-data sampling noise.
    assert (
        0.3 * expected <= actual_count <= 1.7 * expected
    ), f"Expected ~{expected:.0f} sampled pairs, got {actual_count}"
    # Even with noise, sampling should bring us much below the unsampled count.
    assert (
        actual_count < info["estimated_total_pairs"]
    ), "Sampling should reduce the pair count below the unsampled estimate"


def test_em_max_pairs_no_op_when_pairs_already_below(fake_1000_df):
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = _train_and_get_session(
        linker,
        max_pairs=1e9,  # way above true pair count
        probe_proportion=0.2,
    )

    assert session._sample_threshold is None
    assert session._sample_info["sampling_applied"] is False


def test_em_max_pairs_deterministic(fake_1000_df):
    linker_a = _make_dedupe_linker(fake_1000_df)
    linker_a.training.estimate_u_using_random_sampling(max_pairs=1e5)
    session_a = _train_and_get_session(linker_a, max_pairs=500, probe_proportion=0.2)

    linker_b = _make_dedupe_linker(fake_1000_df)
    linker_b.training.estimate_u_using_random_sampling(max_pairs=1e5)
    session_b = _train_and_get_session(linker_b, max_pairs=500, probe_proportion=0.2)

    assert session_a._sample_threshold == session_b._sample_threshold
    assert _count_cvv_rows(session_a) == _count_cvv_rows(session_b)


def test_em_max_pairs_link_only_reduces_pair_count(fake_1000_df):
    linker = _make_link_only_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = _train_and_get_session(linker, max_pairs=100, probe_proportion=0.3)

    assert session._sample_info["sampling_applied"] is True
    actual = _count_cvv_rows(session)
    expected = session._sample_info["expected_pairs_after_sampling"]
    assert (
        0.3 * expected <= actual <= 1.7 * expected
    ), f"link_only: expected ~{expected:.0f}, got {actual}"


def test_em_max_pairs_link_and_dedupe_reduces_pair_count(fake_1000_df):
    linker = _make_link_and_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = _train_and_get_session(linker, max_pairs=500, probe_proportion=0.3)

    assert session._sample_info["sampling_applied"] is True
    actual = _count_cvv_rows(session)
    expected = session._sample_info["expected_pairs_after_sampling"]
    assert (
        0.3 * expected <= actual <= 1.7 * expected
    ), f"link_and_dedupe: expected ~{expected:.0f}, got {actual}"


def test_em_max_pairs_logs_calculations(fake_1000_df, caplog):
    """The sampling helper must emit the per-step calculations to the log."""
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    with caplog.at_level(logging.INFO, logger="splink.internals.em_sampling"):
        _train_and_get_session(linker, max_pairs=500, probe_proportion=0.2)

    msgs = [r.getMessage() for r in caplog.records]
    assert any("Probe at proportion" in m for m in msgs), msgs
    assert any("Estimated total blocked pairs" in m for m in msgs), msgs
    assert any("Chose sampling fraction" in m for m in msgs), msgs


def test_em_training_still_converges_with_max_pairs(fake_1000_df):
    """End-to-end: EM training with max_pairs returns trained m probabilities."""
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = _train_and_get_session(linker, max_pairs=2000, probe_proportion=0.2)
    # Should have run at least one EM iteration with non-empty history.
    assert len(session._core_model_settings_history) >= 1


def test_em_max_pairs_records_sample_metadata(fake_1000_df):
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)

    session = _train_and_get_session(linker, max_pairs=500, probe_proportion=0.2)
    assert session._sample_info["probe_pair_count"] is not None
    assert session._sample_info["actual_probe_fraction"] is not None


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


def test_resolve_em_sample_threshold_rejects_bad_max_pairs(fake_1000_df):
    linker = _make_dedupe_linker(fake_1000_df)
    br = block_on("first_name").get_blocking_rule("duckdb")
    with pytest.raises(ValueError):
        resolve_em_sample_threshold(
            linker=linker, blocking_rule=br, max_pairs=0, probe_proportion=0.1
        )
    with pytest.raises(ValueError):
        resolve_em_sample_threshold(
            linker=linker, blocking_rule=br, max_pairs=-1, probe_proportion=0.1
        )


def test_resolve_em_sample_threshold_rejects_bad_probe_proportion(fake_1000_df):
    linker = _make_dedupe_linker(fake_1000_df)
    br = block_on("first_name").get_blocking_rule("duckdb")
    for bad in (0.0, -0.1, 1.5, 2.0):
        with pytest.raises(ValueError):
            resolve_em_sample_threshold(
                linker=linker,
                blocking_rule=br,
                max_pairs=500,
                probe_proportion=bad,
            )


def test_em_training_max_pairs_rejected_positionally(fake_1000_df):
    linker = _make_dedupe_linker(fake_1000_df)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e5)
    # max_pairs is keyword-only; positional must raise TypeError.
    with pytest.raises(TypeError):
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on("first_name"), False, False, False, True, False, 500
        )


# ---------------------------------------------------------------------------
# SQL-shape regression: positive modulo (no ABS)
# ---------------------------------------------------------------------------


def test_em_sample_filter_uses_positive_modulo():
    from splink.internals.dialects import DuckDBDialect
    from splink.internals.em_sampling import _em_sample_filter_sql
    from splink.internals.input_column import InputColumn

    sql = _em_sample_filter_sql(
        unique_id_cols=[InputColumn("unique_id", sqlglot_dialect_str="duckdb")],
        sample_threshold=42,
        sample_modulus=10_000,
        table_prefix="l",
        dialect=DuckDBDialect(),
    )
    assert "ABS(" not in sql
    assert 'hash(l."unique_id") % 10000' in sql
    assert 'AND hash(l."unique_id") % 10000 < 42' in sql


def test_chunk_assignment_uses_legacy_abs_modulo():
    from splink.internals.chunking import _chunk_assignment_sql
    from splink.internals.dialects import DuckDBDialect
    from splink.internals.input_column import InputColumn

    sql = _chunk_assignment_sql(
        unique_id_cols=[InputColumn("unique_id", sqlglot_dialect_str="duckdb")],
        chunk_num=2,
        num_chunks=5,
        table_prefix="l",
        dialect=DuckDBDialect(),
    )
    assert 'ABS(hash(l."unique_id")) % 5' in sql


def test_em_sample_filter_works_in_duckdb(fake_1000_df):
    import duckdb

    from splink.internals.dialects import DuckDBDialect
    from splink.internals.em_sampling import _em_sample_filter_sql
    from splink.internals.input_column import InputColumn

    sql_filter = _em_sample_filter_sql(
        unique_id_cols=[InputColumn("unique_id", sqlglot_dialect_str="duckdb")],
        sample_threshold=5_000,
        sample_modulus=10_000,
        table_prefix="t",
        dialect=DuckDBDialect(),
    )
    con = duckdb.connect()
    con.register("t", fake_1000_df)
    n = con.execute(f"select count(*) as c from t where 1=1 {sql_filter}").fetchone()[0]
    # Should retain ~half of 1000 rows.
    assert 350 < n < 650


def test_block_using_rules_sqls_adds_em_sample_filter(fake_1000_df):
    from splink.internals.blocking import block_using_rules_sqls

    linker = _make_dedupe_linker(fake_1000_df)
    settings = linker._settings_obj
    br = block_on("first_name").get_blocking_rule("duckdb")

    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=[br],
        link_type=settings._link_type,
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=(settings.column_info_settings.unique_id_input_column),
        sample_threshold=100,
        sample_modulus=10_000,
    )

    assert len(sqls) == 1

    blocked_pairs_sql = sqls[-1]["sql"]
    assert "from __splink__df_concat as l" in blocked_pairs_sql
    assert "inner join __splink__df_concat as r" in blocked_pairs_sql
    assert 'hash(l."unique_id") % 10000 < 100' in blocked_pairs_sql
    assert 'hash(r."unique_id") % 10000 < 100' in blocked_pairs_sql


def test_block_using_rules_sqls_adds_filters_for_distinct_left_and_right_tables(
    fake_1000_df,
):
    from splink.internals.blocking import block_using_rules_sqls

    linker = _make_link_only_linker(fake_1000_df)
    settings = linker._settings_obj
    br = block_on("first_name").get_blocking_rule("duckdb")

    sqls = block_using_rules_sqls(
        input_tablename_l="__splink__df_concat_left",
        input_tablename_r="__splink__df_concat_right",
        blocking_rules=[br],
        link_type="two_dataset_link_only",
        source_dataset_input_column=(
            settings.column_info_settings.source_dataset_input_column
        ),
        unique_id_input_column=(settings.column_info_settings.unique_id_input_column),
        sample_threshold=100,
        sample_modulus=10_000,
    )

    assert len(sqls) == 1

    blocked_pairs_sql = sqls[-1]["sql"]
    assert "from __splink__df_concat_left as l" in blocked_pairs_sql
    assert "inner join __splink__df_concat_right as r" in blocked_pairs_sql
    assert 'hash(l."source_dataset"' in blocked_pairs_sql
    assert 'hash(r."source_dataset"' in blocked_pairs_sql
    assert "% 10000 < 100" in blocked_pairs_sql


def _selected_uids_for_session(session) -> set:
    """Return the set of unique_ids retained after sampling."""
    from splink.internals.em_sampling import _em_sample_filter_sql
    from splink.internals.input_column import InputColumn

    linker = session._original_linker
    db_api = linker._db_api

    sample_sql = _em_sample_filter_sql(
        unique_id_cols=[InputColumn("unique_id", sqlglot_dialect_str="duckdb")],
        sample_threshold=session._sample_threshold,
        sample_modulus=session._sample_modulus,
        table_prefix="t",
        dialect=db_api.sql_dialect,
    )
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.vertically_concatenate import enqueue_df_concat

    pipe = CTEPipeline()
    enqueue_df_concat(linker, pipe)
    pipe.enqueue_sql(
        f"select unique_id from __splink__df_concat as t where 1=1 {sample_sql}",
        "__splink__em_selected_uids",
    )
    df = db_api.sql_pipeline_to_splink_dataframe(pipe)
    rows = df.as_record_dict()
    df.drop_table_from_database_and_remove_from_cache()
    return {r["unique_id"] for r in rows}


def test_em_sample_selects_same_records_across_runs(fake_1000_df):
    linker_a = _make_dedupe_linker(fake_1000_df)
    linker_a.training.estimate_u_using_random_sampling(max_pairs=1e5)
    session_a = _train_and_get_session(linker_a, max_pairs=500, probe_proportion=0.2)
    linker_b = _make_dedupe_linker(fake_1000_df)
    linker_b.training.estimate_u_using_random_sampling(max_pairs=1e5)
    session_b = _train_and_get_session(linker_b, max_pairs=500, probe_proportion=0.2)
    sel_a = _selected_uids_for_session(session_a)
    sel_b = _selected_uids_for_session(session_b)
    assert len(sel_a) > 0 and len(sel_b) > 0
    assert sel_a == sel_b

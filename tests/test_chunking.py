"""Tests for chunked prediction functionality.

Tests that:
1. Chunked predictions produce identical results to non-chunked predictions
2. Pre-caching blocked pairs with inference.compute_blocked_pairs_for_predict_chunk()
   works correctly
3. Cache hits are actually used (not recomputed)
"""

from unittest.mock import patch

import pytest

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.exceptions import SplinkException
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


def _get_comparison_count(result):
    """Get the number of comparisons in a prediction result."""
    return len(result.as_record_dict())


def _sort_predictions(sdf):
    """Sort predictions DataFrame for comparison."""
    return sdf.query_sql(
        "SELECT * FROM {this} ORDER BY unique_id_l, unique_id_r"
    ).as_dict()


@mark_with_dialects_excluding()
def test_chunked_predict_matches_non_chunked(test_helpers, dialect, fake_1000):
    """Test that chunked predictions produce identical results to non-chunked."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    linker = helper.linker_with_registration(fake_1000, settings)

    # Get non-chunked predictions
    predictions_no_chunk = linker.inference.predict(threshold_match_weight=-10)
    df_no_chunk = _sort_predictions(predictions_no_chunk)

    # Invalidate cache to ensure fresh computation
    linker.table_management.invalidate_cache()

    # Get chunked predictions (2x2 grid)
    predictions_chunked = linker.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_chunked = _sort_predictions(predictions_chunked)

    # Results should be identical
    no_chunked_count = len(df_no_chunk["unique_id_l"])
    chunked_count = len(df_no_chunk["unique_id_l"])
    assert (
        no_chunked_count == chunked_count
    ), f"Row count mismatch: {no_chunked_count} vs {chunked_count}"

    # Compare the actual data
    assert df_no_chunk["unique_id_l"] == df_chunked["unique_id_l"]
    assert df_no_chunk["unique_id_r"] == df_chunked["unique_id_r"]


@mark_with_dialects_excluding()
def test_chunked_predict_with_different_chunk_sizes(test_helpers, dialect, fake_1000):
    """Test various chunk size combinations produce consistent results."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    linker = helper.linker_with_registration(fake_1000, settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline)

    # Test different chunk combinations
    chunk_configs = [
        (2, 1),  # 2 left chunks, no right chunking
        (1, 3),  # No left chunking, 3 right chunks
        (3, 2),  # 3 left chunks, 2 right chunks
    ]

    for num_left, num_right in chunk_configs:
        linker.table_management.invalidate_cache()

        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=num_left,
            num_chunks_right=num_right,
        )

        assert (
            _get_comparison_count(predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]


@mark_with_dialects_excluding()
def test_precached_blocked_pairs_same_result(test_helpers, dialect, fake_1000):
    """Test that pre-caching blocked pairs produces same result as no pre-caching."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()

    # First: run without pre-caching
    linker1 = helper.linker_with_registration(fake_1000, settings)
    predictions_no_cache = linker1.inference.predict(threshold_match_weight=-10)
    df_no_cache = _sort_predictions(predictions_no_cache)

    # Second: run with pre-caching
    linker2 = helper.linker_with_registration(fake_1000, settings)
    linker2.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    )
    predictions_with_cache = linker2.inference.predict(threshold_match_weight=-10)
    df_with_cache = _sort_predictions(predictions_with_cache)

    # Results should be identical
    assert len(df_no_cache["unique_id_l"]) == len(df_with_cache["unique_id_l"])
    assert df_no_cache["unique_id_l"] == df_with_cache["unique_id_l"]
    assert df_no_cache["unique_id_r"] == df_with_cache["unique_id_r"]


@mark_with_dialects_excluding()
def test_precached_chunked_blocked_pairs_same_result(test_helpers, dialect, fake_1000):
    """Test that pre-caching chunked blocked pairs produces same result."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()

    # First: run chunked without pre-caching
    linker1 = helper.linker_with_registration(fake_1000, settings)
    predictions_no_cache = linker1.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_no_cache = _sort_predictions(predictions_no_cache)

    # Second: run chunked with pre-caching of all chunks
    linker2 = helper.linker_with_registration(fake_1000, settings)

    # Pre-compute all 4 chunk combinations (2x2)
    for left_chunk_num in [1, 2]:
        for right_chunk_num in [1, 2]:
            linker2.inference.compute_blocked_pairs_for_predict_chunk(
                left_chunk=(left_chunk_num, 2),
                right_chunk=(right_chunk_num, 2),
            )

    predictions_with_cache = linker2.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_with_cache = _sort_predictions(predictions_with_cache)

    # Results should be identical
    assert len(df_no_cache["unique_id_l"]) == len(df_with_cache["unique_id_l"])
    assert df_no_cache["unique_id_l"] == df_with_cache["unique_id_l"]
    assert df_no_cache["unique_id_r"] == df_with_cache["unique_id_r"]


def test_cache_is_hit_for_blocked_pairs(fake_1000):
    """Test that cache is actually hit when blocked pairs are pre-computed.

    This test verifies the cache is used by checking that
    compute_blocked_pairs_from_concat_with_tf is NOT called when
    blocked pairs are already in cache.
    """
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs (populates cache)
    linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    )

    # Verify the cache key exists
    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache

    # Patch the function that computes blocked pairs
    with patch(
        "splink.internals.linker_components.inference.compute_blocked_pairs_from_concat_with_tf"
    ) as mock_compute:
        # Run predict - should use cache, NOT call compute_blocked_pairs
        linker.inference.predict(threshold_match_weight=-10)

        # The compute function should NOT have been called
        mock_compute.assert_not_called()


def test_registered_chunked_blocked_pairs_match_from_scratch(fake_1000):
    """Test chunked predict matches when blocked pairs are loaded from precompute."""
    settings = get_settings_dict()

    # Baseline: run chunked predict from scratch.
    db_api_baseline = DuckDBAPI()
    df_sdf_baseline = db_api_baseline.register(fake_1000)
    linker_baseline = Linker(df_sdf_baseline, settings)
    baseline_predictions = linker_baseline.inference.predict_chunk(
        threshold_match_weight=-10, left_chunk=(1, 2), right_chunk=(1, 2)
    ).as_duckdbpyrelation()
    baseline_count = baseline_predictions.count("*").fetchone()[0]
    baseline_match_weight_sum = baseline_predictions.aggregate(
        "sum(match_weight)"
    ).fetchone()[0]

    # Build blocked pairs externally.
    db_api_source = DuckDBAPI()
    df_sdf_source = db_api_source.register(fake_1000)
    linker_source = Linker(df_sdf_source, settings)

    blocked_pairs = linker_source.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 2), right_chunk=(1, 2)
    ).as_pyarrow_table()

    # Load blocked pairs into a fresh linker and run the same chunked predict.
    db_api_target = DuckDBAPI()
    df_sdf_target = db_api_target.register(fake_1000)
    linker_target = Linker(df_sdf_target, settings)

    linker_target.table_management.register_blocked_pairs_for_predict(
        blocked_pairs, left_chunk=(1, 2), right_chunk=(1, 2)
    )

    loaded_predictions = linker_target.inference.predict_chunk(
        threshold_match_weight=-10,
        left_chunk=(1, 2),
        right_chunk=(1, 2),
    ).as_dict()
    loaded_count = len(loaded_predictions["match_weight"])
    loaded_match_weight_sum = sum(loaded_predictions["match_weight"])

    assert loaded_count == baseline_count
    assert loaded_match_weight_sum == pytest.approx(
        baseline_match_weight_sum, rel=1e-12, abs=1e-12
    )


def test_cache_is_hit_for_chunked_blocked_pairs(fake_1000):
    """Test that cache is hit for pre-computed chunked blocked pairs."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs for specific chunk
    linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 2), right_chunk=(2, 3)
    )

    # Verify the chunk-specific cache key exists
    expected_key = "__splink__blocked_id_pairs_L1of2_R2of3"
    assert expected_key in linker._intermediate_table_cache

    # Patch the function that computes blocked pairs
    with patch(
        "splink.internals.linker_components.inference.compute_blocked_pairs_from_concat_with_tf"
    ) as mock_compute:
        # Run predict_chunk with same chunk params - should use cache
        linker.inference.predict_chunk(
            left_chunk=(1, 2),
            right_chunk=(2, 3),
            threshold_match_weight=-10,
        )

        # The compute function should NOT have been called
        mock_compute.assert_not_called()


def test_cache_key_normalization_1_1(fake_1000):
    """Test that (1,1) chunk normalizes to base cache key."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    # Pre-compute with (1,1) x (1,1) - should normalize to base key
    linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1), right_chunk=(1, 1)
    )

    # Should be stored under base key, not L1of1_R1of1
    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache
    assert (
        "__splink__blocked_id_pairs_L1of1_R1of1" not in linker._intermediate_table_cache
    )


def test_blocked_pairs_not_deleted_when_from_cache(fake_1000):
    """Test that cached blocked pairs are not deleted after predict."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs
    linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    )

    # Run predict
    linker.inference.predict(threshold_match_weight=-10)

    # Blocked pairs should still be in cache
    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache


def test_register_blocked_pairs_requires_chunk_identifiers(fake_1000):
    """Test blocked-pairs registration requires explicit chunk identifiers."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    blocked_pairs = linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    ).as_pyarrow_table()

    with pytest.raises(
        SplinkException,
        match="requires both left_chunk and right_chunk",
    ):
        linker.table_management.register_blocked_pairs_for_predict(blocked_pairs)


def test_predict_errors_when_blocked_pairs_manually_registered(fake_1000):
    """Test predict() is not allowed after manual blocked-pairs registration."""
    settings = get_settings_dict()

    db_api_source = DuckDBAPI()
    df_sdf_source = db_api_source.register(fake_1000)
    linker_source = Linker(df_sdf_source, settings)
    blocked_pairs = linker_source.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    ).as_pyarrow_table()

    db_api_target = DuckDBAPI()
    df_sdf_target = db_api_target.register(fake_1000)
    linker_target = Linker(df_sdf_target, settings)
    linker_target.table_management.register_blocked_pairs_for_predict(
        blocked_pairs,
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    )

    with pytest.raises(SplinkException, match="predict_chunk"):
        linker_target.inference.predict(threshold_match_weight=-10)


def test_blocked_pairs_deleted_when_not_from_cache(fake_1000):
    """Test that non-cached blocked pairs are deleted after predict_chunk."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    # Pre-compute df_concat_with_tf but NOT blocked pairs

    # Run predict_chunk (which computes blocked pairs internally)
    linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(1, 3),
        threshold_match_weight=-10,
    )

    # Blocked pairs should NOT be in cache (deleted after use)
    cache_key = "__splink__blocked_id_pairs_L1of2_R1of3"
    assert cache_key not in linker._intermediate_table_cache


@mark_with_dialects_excluding()
def test_chunked_predict_link_only(test_helpers, dialect, fake_1000):
    """Test chunked predictions work correctly with link_only (two datasets)."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_only"

    # Split into two datasets using modulo arithmetic
    df_1 = fake_1000.take(list(range(0, 1000, 2)))
    df_2 = fake_1000.take(list(range(1, 1000, 2)))

    linker = helper.linker_with_registration([df_1, df_2], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline)

    # Test different chunk combinations
    chunk_configs = [
        (2, 1),  # 2 left chunks, no right chunking
        (1, 3),  # No left chunking, 3 right chunks
        (3, 2),  # 3 left chunks, 2 right chunks
    ]

    for num_left, num_right in chunk_configs:
        linker.table_management.invalidate_cache()

        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=num_left,
            num_chunks_right=num_right,
        )

        assert (
            _get_comparison_count(predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]


@mark_with_dialects_excluding()
def test_chunked_predict_link_only_three_datasets(test_helpers, dialect, fake_1000):
    """Test chunked predictions work correctly with link_only (three datasets).

    Two datasets is a special case, so we test with three datasets as well.
    """
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_only"

    # Split into three datasets using modulo arithmetic
    df_1 = fake_1000.take(list(range(0, 1000, 3)))
    df_2 = fake_1000.take(list(range(1, 1000, 3)))
    df_3 = fake_1000.take(list(range(2, 1000, 3)))

    linker = helper.linker_with_registration([df_1, df_2, df_3], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline)

    # Test different chunk combinations
    chunk_configs = [
        (2, 1),  # 2 left chunks, no right chunking
        (1, 3),  # No left chunking, 3 right chunks
        (3, 2),  # 3 left chunks, 2 right chunks
    ]

    for num_left, num_right in chunk_configs:
        linker.table_management.invalidate_cache()

        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=num_left,
            num_chunks_right=num_right,
        )

        assert (
            _get_comparison_count(predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]


@mark_with_dialects_excluding()
def test_chunked_predict_link_and_dedupe(test_helpers, dialect, fake_1000):
    """Test chunked predictions work correctly with link_and_dedupe (two datasets)."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_and_dedupe"

    # Split into two datasets using modulo arithmetic
    df_1 = fake_1000.take(list(range(0, 1000, 2)))
    df_2 = fake_1000.take(list(range(1, 1000, 2)))

    linker = helper.linker_with_registration([df_1, df_2], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline)

    # Test different chunk combinations
    chunk_configs = [
        (2, 1),  # 2 left chunks, no right chunking
        (1, 3),  # No left chunking, 3 right chunks
        (3, 2),  # 3 left chunks, 2 right chunks
    ]

    for num_left, num_right in chunk_configs:
        linker.table_management.invalidate_cache()

        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=num_left,
            num_chunks_right=num_right,
        )

        assert (
            _get_comparison_count(predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]


@mark_with_dialects_excluding()
def test_chunked_predict_link_and_dedupe_three_datasets(
    test_helpers, dialect, fake_1000
):
    """Test chunked predictions work correctly with link_and_dedupe (three datasets).

    Two datasets is a special case, so we test with three datasets as well.
    """
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_and_dedupe"

    # Split into three datasets using modulo arithmetic
    df_1 = fake_1000.take(list(range(0, 1000, 3)))
    df_2 = fake_1000.take(list(range(1, 1000, 3)))
    df_3 = fake_1000.take(list(range(2, 1000, 3)))

    linker = helper.linker_with_registration([df_1, df_2, df_3], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline)

    # Test different chunk combinations
    chunk_configs = [
        (2, 1),  # 2 left chunks, no right chunking
        (1, 3),  # No left chunking, 3 right chunks
        (3, 2),  # 3 left chunks, 2 right chunks
    ]

    for num_left, num_right in chunk_configs:
        linker.table_management.invalidate_cache()

        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=num_left,
            num_chunks_right=num_right,
        )

        assert (
            _get_comparison_count(predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]

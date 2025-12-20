"""Tests for chunked prediction functionality.

Tests that:
1. Chunked predictions produce identical results to non-chunked predictions
2. Pre-caching blocked pairs with compute_blocked_pairs_for_predict() works correctly
3. Cache hits are actually used (not recomputed)
"""

from unittest.mock import patch

import pandas as pd

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


def _get_comparison_count(linker, result):
    """Get the number of comparisons in a prediction result."""
    return result.as_pandas_dataframe().shape[0]


def _sort_predictions(df):
    """Sort predictions DataFrame for comparison."""
    id_cols = ["unique_id_l", "unique_id_r"]
    return df.sort_values(id_cols).reset_index(drop=True)


@mark_with_dialects_excluding()
def test_chunked_predict_matches_non_chunked(test_helpers, dialect):
    """Test that chunked predictions produce identical results to non-chunked."""
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()
    linker = helper.linker_with_registration(df, settings)

    # Get non-chunked predictions
    predictions_no_chunk = linker.inference.predict(threshold_match_weight=-10)
    df_no_chunk = _sort_predictions(predictions_no_chunk.as_pandas_dataframe())

    # Invalidate cache to ensure fresh computation
    linker.table_management.invalidate_cache()

    # Get chunked predictions (2x2 grid)
    predictions_chunked = linker.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_chunked = _sort_predictions(predictions_chunked.as_pandas_dataframe())

    # Results should be identical
    assert len(df_no_chunk) == len(
        df_chunked
    ), f"Row count mismatch: {len(df_no_chunk)} vs {len(df_chunked)}"

    # Compare the actual data (ignoring floating point precision issues)
    pd.testing.assert_frame_equal(
        df_no_chunk[["unique_id_l", "unique_id_r"]],
        df_chunked[["unique_id_l", "unique_id_r"]],
    )


@mark_with_dialects_excluding()
def test_chunked_predict_with_different_chunk_sizes(test_helpers, dialect):
    """Test various chunk size combinations produce consistent results."""
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()
    linker = helper.linker_with_registration(df, settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(linker, predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline.as_pandas_dataframe())

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
            _get_comparison_count(linker, predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions.as_pandas_dataframe())
        pd.testing.assert_frame_equal(
            df_baseline[["unique_id_l", "unique_id_r"]],
            df_chunked[["unique_id_l", "unique_id_r"]],
        )


@mark_with_dialects_excluding()
def test_precached_blocked_pairs_same_result(test_helpers, dialect):
    """Test that pre-caching blocked pairs produces same result as no pre-caching."""
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()

    # First: run without pre-caching
    linker1 = helper.linker_with_registration(df, settings)
    predictions_no_cache = linker1.inference.predict(threshold_match_weight=-10)
    df_no_cache = _sort_predictions(predictions_no_cache.as_pandas_dataframe())

    # Second: run with pre-caching
    linker2 = helper.linker_with_registration(df, settings)
    linker2.table_management.compute_df_concat_with_tf()
    linker2.table_management.compute_blocked_pairs_for_predict()
    predictions_with_cache = linker2.inference.predict(threshold_match_weight=-10)
    df_with_cache = _sort_predictions(predictions_with_cache.as_pandas_dataframe())

    # Results should be identical
    assert len(df_no_cache) == len(df_with_cache)
    pd.testing.assert_frame_equal(
        df_no_cache[["unique_id_l", "unique_id_r"]],
        df_with_cache[["unique_id_l", "unique_id_r"]],
    )


@mark_with_dialects_excluding()
def test_precached_chunked_blocked_pairs_same_result(test_helpers, dialect):
    """Test that pre-caching chunked blocked pairs produces same result."""
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = get_settings_dict()

    # First: run chunked without pre-caching
    linker1 = helper.linker_with_registration(df, settings)
    predictions_no_cache = linker1.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_no_cache = _sort_predictions(predictions_no_cache.as_pandas_dataframe())

    # Second: run chunked with pre-caching of all chunks
    linker2 = helper.linker_with_registration(df, settings)
    linker2.table_management.compute_df_concat_with_tf()

    # Pre-compute all 4 chunk combinations (2x2)
    for left_chunk_num in [1, 2]:
        for right_chunk_num in [1, 2]:
            linker2.table_management.compute_blocked_pairs_for_predict(
                left_chunk=(left_chunk_num, 2),
                right_chunk=(right_chunk_num, 2),
            )

    predictions_with_cache = linker2.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
    )
    df_with_cache = _sort_predictions(predictions_with_cache.as_pandas_dataframe())

    # Results should be identical
    assert len(df_no_cache) == len(df_with_cache)
    pd.testing.assert_frame_equal(
        df_no_cache[["unique_id_l", "unique_id_r"]],
        df_with_cache[["unique_id_l", "unique_id_r"]],
    )


def test_cache_is_hit_for_blocked_pairs():
    """Test that cache is actually hit when blocked pairs are pre-computed.

    This test verifies the cache is used by checking that
    compute_blocked_pairs_from_concat_with_tf is NOT called when
    blocked pairs are already in cache.
    """
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs (populates cache)
    linker.table_management.compute_df_concat_with_tf()
    linker.table_management.compute_blocked_pairs_for_predict()

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


def test_cache_is_hit_for_chunked_blocked_pairs():
    """Test that cache is hit for pre-computed chunked blocked pairs."""
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs for specific chunk
    linker.table_management.compute_df_concat_with_tf()
    linker.table_management.compute_blocked_pairs_for_predict(
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


def test_cache_key_normalization_1_1():
    """Test that (1,1) chunk normalizes to base cache key."""
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    # Pre-compute with (1,1) x (1,1) - should normalize to base key
    linker.table_management.compute_df_concat_with_tf()
    linker.table_management.compute_blocked_pairs_for_predict(
        left_chunk=(1, 1), right_chunk=(1, 1)
    )

    # Should be stored under base key, not L1of1_R1of1
    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache
    assert (
        "__splink__blocked_id_pairs_L1of1_R1of1" not in linker._intermediate_table_cache
    )


def test_blocked_pairs_not_deleted_when_from_cache():
    """Test that cached blocked pairs are not deleted after predict."""
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    # Pre-compute blocked pairs
    linker.table_management.compute_df_concat_with_tf()
    linker.table_management.compute_blocked_pairs_for_predict()

    # Run predict
    linker.inference.predict(threshold_match_weight=-10)

    # Blocked pairs should still be in cache
    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache


def test_blocked_pairs_deleted_when_not_from_cache():
    """Test that non-cached blocked pairs are deleted after predict_chunk."""
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)

    # Pre-compute df_concat_with_tf but NOT blocked pairs
    linker.table_management.compute_df_concat_with_tf()

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
def test_chunked_predict_link_only(test_helpers, dialect):
    """Test chunked predictions work correctly with link_only (two datasets)."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_only"

    # Split into two datasets using modulo arithmetic
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df1_pd = df_pd[df_pd.index % 2 == 0].copy().reset_index(drop=True)
    df2_pd = df_pd[df_pd.index % 2 == 1].copy().reset_index(drop=True)

    df1 = helper.convert_frame(df1_pd)
    df2 = helper.convert_frame(df2_pd)

    linker = helper.linker_with_registration([df1, df2], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(linker, predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline.as_pandas_dataframe())

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
            _get_comparison_count(linker, predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions.as_pandas_dataframe())
        pd.testing.assert_frame_equal(
            df_baseline[["unique_id_l", "unique_id_r"]],
            df_chunked[["unique_id_l", "unique_id_r"]],
        )


@mark_with_dialects_excluding()
def test_chunked_predict_link_only_three_datasets(test_helpers, dialect):
    """Test chunked predictions work correctly with link_only (three datasets).

    Two datasets is a special case, so we test with three datasets as well.
    """
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_only"

    # Split into three datasets using modulo arithmetic
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df1_pd = df_pd[df_pd.index % 3 == 0].copy().reset_index(drop=True)
    df2_pd = df_pd[df_pd.index % 3 == 1].copy().reset_index(drop=True)
    df3_pd = df_pd[df_pd.index % 3 == 2].copy().reset_index(drop=True)

    df1 = helper.convert_frame(df1_pd)
    df2 = helper.convert_frame(df2_pd)
    df3 = helper.convert_frame(df3_pd)

    linker = helper.linker_with_registration([df1, df2, df3], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(linker, predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline.as_pandas_dataframe())

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
            _get_comparison_count(linker, predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions.as_pandas_dataframe())
        pd.testing.assert_frame_equal(
            df_baseline[["unique_id_l", "unique_id_r"]],
            df_chunked[["unique_id_l", "unique_id_r"]],
        )


@mark_with_dialects_excluding()
def test_chunked_predict_link_and_dedupe(test_helpers, dialect):
    """Test chunked predictions work correctly with link_and_dedupe (two datasets)."""
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_and_dedupe"

    # Split into two datasets using modulo arithmetic
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df1_pd = df_pd[df_pd.index % 2 == 0].copy().reset_index(drop=True)
    df2_pd = df_pd[df_pd.index % 2 == 1].copy().reset_index(drop=True)

    df1 = helper.convert_frame(df1_pd)
    df2 = helper.convert_frame(df2_pd)

    linker = helper.linker_with_registration([df1, df2], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(linker, predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline.as_pandas_dataframe())

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
            _get_comparison_count(linker, predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions.as_pandas_dataframe())
        pd.testing.assert_frame_equal(
            df_baseline[["unique_id_l", "unique_id_r"]],
            df_chunked[["unique_id_l", "unique_id_r"]],
        )


@mark_with_dialects_excluding()
def test_chunked_predict_link_and_dedupe_three_datasets(test_helpers, dialect):
    """Test chunked predictions work correctly with link_and_dedupe (three datasets).

    Two datasets is a special case, so we test with three datasets as well.
    """
    helper = test_helpers[dialect]

    settings = get_settings_dict()
    settings["link_type"] = "link_and_dedupe"

    # Split into three datasets using modulo arithmetic
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df1_pd = df_pd[df_pd.index % 3 == 0].copy().reset_index(drop=True)
    df2_pd = df_pd[df_pd.index % 3 == 1].copy().reset_index(drop=True)
    df3_pd = df_pd[df_pd.index % 3 == 2].copy().reset_index(drop=True)

    df1 = helper.convert_frame(df1_pd)
    df2 = helper.convert_frame(df2_pd)
    df3 = helper.convert_frame(df3_pd)

    linker = helper.linker_with_registration([df1, df2, df3], settings)

    # Get baseline predictions
    predictions_baseline = linker.inference.predict(threshold_match_weight=-10)
    baseline_count = _get_comparison_count(linker, predictions_baseline)
    df_baseline = _sort_predictions(predictions_baseline.as_pandas_dataframe())

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
            _get_comparison_count(linker, predictions) == baseline_count
        ), f"Chunk config ({num_left}, {num_right}) produced different count"

        df_chunked = _sort_predictions(predictions.as_pandas_dataframe())
        pd.testing.assert_frame_equal(
            df_baseline[["unique_id_l", "unique_id_r"]],
            df_chunked[["unique_id_l", "unique_id_r"]],
        )

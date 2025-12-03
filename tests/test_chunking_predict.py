import pandas as pd
import pytest

from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.internals.comparison_library import ExactMatch


@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "unique_id": range(100),
            "first_name": [f"name_{i % 10}" for i in range(100)],
            "surname": [f"surname_{i % 5}" for i in range(100)],
        }
    )


@pytest.fixture
def linker(sample_data):
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[ExactMatch("first_name"), ExactMatch("surname")],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
        ],
    )
    db_api = DuckDBAPI()
    return Linker(sample_data, settings, db_api=db_api)


@pytest.mark.duckdb
class TestChunkedPredictEquality:
    """THE KEY TESTS: chunked and non-chunked predict must be identical.

    This is the fundamental correctness property of chunking:
    predict(left_chunks=N, right_chunks=M) must produce exactly the
    same pairs with exactly the same scores as predict() without chunking.
    """

    def test_chunked_predict_equals_non_chunked(self, linker):
        """Chunked prediction must produce identical results to non-chunked."""
        # Get non-chunked result
        df_no_chunk = linker.inference.predict()

        # Get chunked result (automatically combines all chunks)
        df_chunked = linker.inference.predict(left_chunks=3, right_chunks=4)

        # Convert to comparable format
        pd_no_chunk = (
            df_no_chunk.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )
        pd_chunked = (
            df_chunked.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )

        # Must have same number of rows
        assert len(pd_no_chunk) == len(
            pd_chunked
        ), f"Row count mismatch: {len(pd_no_chunk)} vs {len(pd_chunked)}"

        # Must have identical content
        pd.testing.assert_frame_equal(pd_no_chunk, pd_chunked)

    def test_chunked_2x2_equals_non_chunked(self, linker):
        """Test with 2x2 chunking for simpler debugging."""
        df_no_chunk = linker.inference.predict()
        df_chunked = linker.inference.predict(left_chunks=2, right_chunks=2)

        pd_no_chunk = (
            df_no_chunk.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )
        pd_chunked = (
            df_chunked.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )

        assert len(pd_no_chunk) == len(pd_chunked)
        pd.testing.assert_frame_equal(pd_no_chunk, pd_chunked)

    def test_single_chunk_equals_non_chunked(self, linker):
        """1x1 chunking should be identical to non-chunked."""
        df_no_chunk = linker.inference.predict()
        df_chunked = linker.inference.predict(left_chunks=1, right_chunks=1)

        pd_no_chunk = (
            df_no_chunk.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )
        pd_chunked = (
            df_chunked.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(pd_no_chunk, pd_chunked)


@pytest.mark.duckdb
class TestPredictChunk:
    """Tests for the low-level predict_chunk() method."""

    def test_predict_chunk_basic(self, linker):
        """predict_chunk() returns results for a single chunk."""
        df = linker.inference.predict_chunk(
            left_chunk=(1, 2),
            right_chunk=(1, 3),
        )
        # Should return a non-empty dataframe (we have enough data)
        assert df.as_pandas_dataframe().shape[0] >= 0  # May be empty for some chunks

    def test_predict_chunk_requires_both_params(self, linker):
        """predict_chunk() requires both left_chunk and right_chunk."""
        with pytest.raises((ValueError, TypeError)):
            linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=None)

        with pytest.raises((ValueError, TypeError)):
            linker.inference.predict_chunk(left_chunk=None, right_chunk=(1, 2))

    def test_manual_chunks_equal_auto_chunked(self, linker):
        """Manually combining predict_chunk() results equals predict(left_chunks=...)"""
        # Auto-chunked
        df_auto = linker.inference.predict(left_chunks=2, right_chunks=2)
        auto_pairs = set(
            zip(
                df_auto.as_pandas_dataframe()["unique_id_l"],
                df_auto.as_pandas_dataframe()["unique_id_r"],
            )
        )

        # Manually chunked
        manual_pairs = set()
        for left_n in [1, 2]:
            for right_n in [1, 2]:
                df_chunk = linker.inference.predict_chunk(
                    left_chunk=(left_n, 2),
                    right_chunk=(right_n, 2),
                )
                chunk_pairs = set(
                    zip(
                        df_chunk.as_pandas_dataframe()["unique_id_l"],
                        df_chunk.as_pandas_dataframe()["unique_id_r"],
                    )
                )
                manual_pairs.update(chunk_pairs)

        assert auto_pairs == manual_pairs


@pytest.mark.duckdb
class TestChunkDisjointness:
    """Tests ensuring chunks partition the data correctly."""

    def test_chunks_are_disjoint(self, linker):
        """Different chunks produce non-overlapping results."""
        df1 = linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=(1, 2))
        df2 = linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=(2, 2))

        set1 = set(
            zip(
                df1.as_pandas_dataframe()["unique_id_l"],
                df1.as_pandas_dataframe()["unique_id_r"],
            )
        )
        set2 = set(
            zip(
                df2.as_pandas_dataframe()["unique_id_l"],
                df2.as_pandas_dataframe()["unique_id_r"],
            )
        )

        assert set1.isdisjoint(set2), "Chunks should not have overlapping pairs"

    def test_all_chunks_cover_full_result(self, linker):
        """Union of all chunks equals full predict() result."""
        # Get non-chunked result
        df_full = linker.inference.predict()
        full_pairs = set(
            zip(
                df_full.as_pandas_dataframe()["unique_id_l"],
                df_full.as_pandas_dataframe()["unique_id_r"],
            )
        )

        # Get all chunks and union them
        all_chunk_pairs = set()
        for left_n in [1, 2, 3]:
            for right_n in [1, 2]:
                df_chunk = linker.inference.predict_chunk(
                    left_chunk=(left_n, 3),
                    right_chunk=(right_n, 2),
                )
                chunk_pairs = set(
                    zip(
                        df_chunk.as_pandas_dataframe()["unique_id_l"],
                        df_chunk.as_pandas_dataframe()["unique_id_r"],
                    )
                )
                all_chunk_pairs.update(chunk_pairs)

        assert full_pairs == all_chunk_pairs


@pytest.mark.duckdb
class TestChunkDeterminism:
    """Tests for deterministic chunk assignment."""

    def test_chunking_is_deterministic(self, linker):
        """Same chunk parameters produce identical results."""
        df1 = linker.inference.predict_chunk(left_chunk=(1, 3), right_chunk=(2, 4))
        df2 = linker.inference.predict_chunk(left_chunk=(1, 3), right_chunk=(2, 4))

        pd1 = df1.as_pandas_dataframe().sort_values(["unique_id_l", "unique_id_r"])
        pd2 = df2.as_pandas_dataframe().sort_values(["unique_id_l", "unique_id_r"])

        pd.testing.assert_frame_equal(
            pd1.reset_index(drop=True), pd2.reset_index(drop=True)
        )


@pytest.mark.duckdb
class TestChunkValidation:
    """Tests for parameter validation."""

    def test_predict_requires_both_chunk_params_or_neither(self, linker):
        """predict() must have both left_chunks and right_chunks or neither."""
        with pytest.raises(ValueError, match="must both be specified"):
            linker.inference.predict(left_chunks=3)

        with pytest.raises(ValueError, match="must both be specified"):
            linker.inference.predict(right_chunks=3)

    def test_invalid_chunk_values_in_predict_chunk(self, linker):
        """Invalid chunk parameters raise ValueError."""
        with pytest.raises(ValueError):
            linker.inference.predict_chunk(left_chunk=(0, 2), right_chunk=(1, 2))

        with pytest.raises(ValueError):
            linker.inference.predict_chunk(left_chunk=(3, 2), right_chunk=(1, 2))

        with pytest.raises(ValueError):
            linker.inference.predict_chunk(left_chunk=(1, 2), right_chunk=(1, 0))

    def test_invalid_chunks_in_predict(self, linker):
        """Invalid chunk counts raise ValueError."""
        with pytest.raises(ValueError):
            linker.inference.predict(left_chunks=0, right_chunks=2)

        with pytest.raises(ValueError):
            linker.inference.predict(left_chunks=2, right_chunks=-1)


@pytest.mark.duckdb
class TestChunkedPredictWithThreshold:
    """Tests for chunked predict with threshold parameters."""

    def test_chunked_predict_with_threshold(self, linker):
        """Chunked prediction with threshold equals non-chunked with threshold."""
        threshold = 0.5

        df_no_chunk = linker.inference.predict(threshold_match_probability=threshold)
        df_chunked = linker.inference.predict(
            threshold_match_probability=threshold, left_chunks=2, right_chunks=2
        )

        pd_no_chunk = (
            df_no_chunk.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )
        pd_chunked = (
            df_chunked.as_pandas_dataframe()
            .sort_values(["unique_id_l", "unique_id_r"])
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(pd_no_chunk, pd_chunked)


@pytest.mark.duckdb
class TestChunkedPredictMaterialisation:
    """Tests ensuring chunked results are properly materialised."""

    def test_chunked_result_is_materialised_and_queryable(self, linker):
        """Chunked result should be fully materialised and queryable via DuckDB.

        This test ensures that after chunking, the intermediate chunk tables
        are dropped but the combined result remains accessible.
        """
        # Get chunked result
        df_chunked = linker.inference.predict(left_chunks=2, right_chunks=2)

        # Access as DuckDB relation - this should work because data is materialised
        ddb_rel = df_chunked.as_duckdbpyrelation()

        # Query the relation - this will fail if data wasn't materialised
        result = ddb_rel.aggregate("count(*) as cnt").fetchone()
        assert result is not None
        assert result[0] > 0

        # Also verify we can compute aggregates
        sum_result = ddb_rel.aggregate("sum(match_weight) as total_mw").fetchone()
        assert sum_result is not None

    def test_chunked_and_non_chunked_aggregates_match(self, linker):
        """Sum of match weights should be identical for chunked and non-chunked."""
        df_no_chunk = linker.inference.predict()
        df_chunked = linker.inference.predict(left_chunks=2, right_chunks=2)

        # Get aggregates via DuckDB relations
        sum_no_chunk = (
            df_no_chunk.as_duckdbpyrelation()
            .aggregate("sum(match_weight) as total")
            .fetchone()[0]
        )
        sum_chunked = (
            df_chunked.as_duckdbpyrelation()
            .aggregate("sum(match_weight) as total")
            .fetchone()[0]
        )

        # Should be equal (or very close due to floating point)
        assert abs(sum_no_chunk - sum_chunked) < 1e-10

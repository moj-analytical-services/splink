"""Tests for chunked prediction functionality.

Tests that:
1. Chunked predictions produce identical results to non-chunked predictions
2. Pre-caching blocked pairs with inference.compute_blocked_pairs_for_predict_chunk()
   works correctly
3. Cache hits are actually used (not recomputed)
"""

from unittest.mock import patch

import pytest

import splink.comparison_library as cl
from splink.internals.chunking import (
    _chunk_assignment_expression,
    _chunk_assignment_sql,
    _is_effective_chunk,
)
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_independent_sqls,
)
from splink.internals.dialects import DuckDBDialect
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.duckdb.database_api_with_profiling import (
    DuckDBAPIWithProfiling,
)
from splink.internals.exceptions import SplinkException
from splink.internals.input_column import InputColumn
from splink.internals.linker import Linker
from splink.internals.pipeline import CTEPipeline
from splink.internals.sqlite.database_api import SQLiteAPI

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding


def _get_comparison_count(result):
    """Get the number of comparisons in a prediction result."""
    return len(result.as_record_list())


def _sort_predictions(sdf):
    """Sort predictions DataFrame for comparison."""
    return sdf.query_sql(
        "SELECT * FROM {this} ORDER BY unique_id_l, unique_id_r"
    ).as_dict()


def test_chunk_assignment_expression_matches_blocking_fragment():
    unique_id_cols = [InputColumn("unique_id", sqlglot_dialect_str="duckdb")]
    dialect = DuckDBDialect()

    expression = _chunk_assignment_expression(unique_id_cols, 2, 5, "l", dialect)

    assert _chunk_assignment_sql(unique_id_cols, 2, 5, "l", dialect) == (
        f" AND {expression}"
    )
    assert _is_effective_chunk((1, 2))
    assert not _is_effective_chunk((1, 1))
    assert not _is_effective_chunk(None)


def test_pipeline_materialized_hint_is_opt_in():
    pipeline = CTEPipeline()
    pipeline.enqueue_sql_materialized("select 1 as x", "materialized_input")
    pipeline.enqueue_sql("select * from materialized_input", "output")

    sql = pipeline.generate_cte_pipeline_sql()

    assert "materialized_input as MATERIALIZED" in sql


def test_duckdb_pipeline_table_tracks_cleanup():
    db_api = DuckDBAPI()
    pipeline = CTEPipeline()
    pipeline.enqueue_sql("select 1 as x", "__splink__pipeline_test")
    table = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    table_info = db_api.duckdb_con.execute(
        """
        select temporary
        from duckdb_tables()
        where table_name = ?
        """,
        [table.physical_name],
    ).fetchone()
    assert table_info == (False,)
    assert table.physical_name in db_api._created_tables

    table.drop_table_from_database_and_remove_from_cache()

    assert not db_api.table_exists_in_database(table.physical_name)


def test_duckdb_profiling_captures_pipeline_tables(tmp_path):
    db_api = DuckDBAPIWithProfiling(query_profiling_dir=tmp_path)
    pipeline = CTEPipeline()
    pipeline.enqueue_sql("select 1 as x", "__splink__pipeline_profile_test")
    db_api.sql_pipeline_to_splink_dataframe(pipeline)

    profiles = list(tmp_path.glob("*.txt"))
    assert len(profiles) == 1
    assert "Total Time" in profiles[0].read_text()


def test_independent_hydration_sql_preserves_payload_order():
    sqls = compute_comparison_vector_values_from_id_pairs_independent_sqls(
        [
            '"l"."unique_id" AS "unique_id_l"',
            '"r"."unique_id" AS "unique_id_r"',
            '"l"."first_name" AS "first_name_l"',
            '"r"."first_name" AS "first_name_r"',
        ],
        ["unique_id_l", "unique_id_r"],
        "left_input",
        "right_input",
        None,
        InputColumn("unique_id", sqlglot_dialect_str="duckdb"),
    )

    assert sqls[0]["materialized"] is True
    assert sqls[1]["materialized"] is True
    blocked_sql = str(sqls[2]["sql"])
    assert blocked_sql.index('hydrated_l."unique_id_l"') < blocked_sql.index(
        'hydrated_r."unique_id_r"'
    )
    assert blocked_sql.index('hydrated_r."unique_id_r"') < blocked_sql.index(
        'hydrated_l."first_name_l"'
    )


def test_explicit_independent_hydration_matches_normal(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    input_df = db_api.register(fake_1000)
    linker = Linker(input_df, settings)

    normal = _sort_predictions(
        linker.inference.predict(
            threshold_match_weight=-10,
            use_independent_hydration=False,
        )
    )
    linker.table_management.invalidate_cache()
    independent = _sort_predictions(
        linker.inference.predict(
            threshold_match_weight=-10,
            use_independent_hydration=True,
        )
    )

    assert normal == independent


def test_effective_chunk_defaults_to_independent_physical_hydration(fake_1000):
    settings = get_settings_dict()

    db_api_default = DuckDBAPI()
    default_linker = Linker(db_api_default.register(fake_1000), settings)
    default_predictions = default_linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
        threshold_match_weight=-10,
    )

    db_api_normal = DuckDBAPI()
    normal_linker = Linker(db_api_normal.register(fake_1000), settings)
    normal_predictions = normal_linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
        threshold_match_weight=-10,
        use_independent_hydration=False,
    )

    assert _sort_predictions(default_predictions) == _sort_predictions(
        normal_predictions
    )
    assert "__splink__left_records as MATERIALIZED" in (
        default_predictions.sql_used_to_create
    )
    assert "__splink__df_predict_input_l_1_of_2" in (
        default_predictions.sql_used_to_create
    )
    assert "__splink__df_predict_input_r_2_of_3" in (
        default_predictions.sql_used_to_create
    )

    remaining_tables = {
        row[0]
        for row in db_api_default.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any("__splink__df_predict_input_" in name for name in remaining_tables)


def test_compute_blocked_pairs_physical_chunk_cleans_up_inputs(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    blocked_pairs = linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
    )

    assert blocked_pairs.as_duckdbpyrelation().count("*").fetchone()[0] > 0
    remaining_tables = {
        row[0]
        for row in db_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any("__splink__df_predict_input_" in name for name in remaining_tables)


def test_chunked_predict_reuses_left_physical_inputs(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    with patch.object(db_api, "_execute_sql", wraps=db_api._execute_sql) as execute_sql:
        linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=2,
            num_chunks_right=2,
        )

    chunk_calls = [
        call
        for call in execute_sql.call_args_list
        if "__splink__df_predict_input_" in call.args[1]
    ]
    assert len(chunk_calls) == 4


def test_physical_chunking_supports_exploding_blocking_rules():
    records = [
        {"unique_id": 1, "tokens": ["a", "b"]},
        {"unique_id": 2, "tokens": ["a"]},
        {"unique_id": 3, "tokens": ["b"]},
        {"unique_id": 4, "tokens": ["c"]},
    ]
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": "l.tokens = r.tokens",
                "arrays_to_explode": ["tokens"],
            }
        ],
        "comparisons": [cl.ArrayIntersectAtSizes("tokens", [1])],
    }

    normal_api = DuckDBAPI()
    normal_linker = Linker(normal_api.register(records), settings)
    normal = _sort_predictions(
        normal_linker.inference.predict(threshold_match_weight=-10)
    )

    chunked_api = DuckDBAPI()
    chunked_linker = Linker(chunked_api.register(records), settings)
    chunked = _sort_predictions(
        chunked_linker.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=2,
            num_chunks_right=2,
        )
    )

    assert normal == chunked


def test_no_effective_chunk_preserves_normal_path(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    predictions = linker.inference.predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
        threshold_match_weight=-10,
    )

    assert "__splink__left_records as MATERIALIZED" not in (
        predictions.sql_used_to_create
    )
    assert "__splink__df_predict_input_" not in predictions.sql_used_to_create


def test_predict_chunk_failure_cleans_up_physical_inputs(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    with patch.object(
        linker.inference,
        "_get_or_compute_blocked_pairs_for_predict_chunk",
        side_effect=RuntimeError("injected failure"),
    ):
        with pytest.raises(RuntimeError, match="injected failure"):
            linker.inference.predict_chunk(
                left_chunk=(1, 2),
                right_chunk=(2, 3),
            )

    remaining_tables = {
        row[0]
        for row in db_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any("__splink__df_predict_input_" in name for name in remaining_tables)


def test_chunked_predict_failure_cleans_up_physical_inputs(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    with patch.object(
        linker.inference,
        "_predict_chunk",
        side_effect=RuntimeError("injected failure"),
    ):
        with pytest.raises(RuntimeError, match="injected failure"):
            linker.inference.predict(
                num_chunks_left=2,
                num_chunks_right=2,
            )

    remaining_tables = {
        row[0]
        for row in db_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any("__splink__df_predict_input_" in name for name in remaining_tables)


def test_later_chunk_failure_cleans_up_prior_prediction_tables(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)
    original_predict_chunk = linker.inference._predict_chunk
    call_count = 0

    def fail_second_chunk(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("injected second chunk failure")
        return original_predict_chunk(*args, **kwargs)

    with patch.object(
        linker.inference,
        "_predict_chunk",
        side_effect=fail_second_chunk,
    ):
        with pytest.raises(RuntimeError, match="injected second chunk failure"):
            linker.inference.predict(
                threshold_match_weight=-10,
                num_chunks_left=2,
                num_chunks_right=2,
            )

    remaining_tables = {
        row[0]
        for row in db_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any("__splink__df_predict_" in name for name in remaining_tables)


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
    assert no_chunked_count == chunked_count, (
        f"Row count mismatch: {no_chunked_count} vs {chunked_count}"
    )

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

        assert _get_comparison_count(predictions) == baseline_count, (
            f"Chunk config ({num_left}, {num_right}) produced different count"
        )

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
    """Test predict() matches when blocked pairs are loaded from precompute."""
    settings = get_settings_dict()

    # Baseline: run predict from scratch (Splink computes blocking internally).
    db_api_baseline = DuckDBAPI()
    df_sdf_baseline = db_api_baseline.register(fake_1000)
    linker_baseline = Linker(df_sdf_baseline, settings)
    baseline_predictions = linker_baseline.inference.predict(
        threshold_match_weight=-10
    ).as_duckdbpyrelation()
    baseline_count = baseline_predictions.count("*").fetchone()[0]
    baseline_match_weight_sum = baseline_predictions.aggregate(
        "sum(match_weight)"
    ).fetchone()[0]

    # Build the full blocked pairs table externally.
    db_api_source = DuckDBAPI()
    df_sdf_source = db_api_source.register(fake_1000)
    linker_source = Linker(df_sdf_source, settings)

    blocked_pairs_arrow = (
        linker_source.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    # Register the full table into a fresh linker and run predict.
    db_api_target = DuckDBAPI()
    df_sdf_target = db_api_target.register(fake_1000)
    linker_target = Linker(df_sdf_target, settings)

    blocked_pairs = db_api_target.register(blocked_pairs_arrow)
    linker_target.table_management.register_blocked_pairs_for_predict(blocked_pairs)

    loaded_predictions = linker_target.inference.predict(
        threshold_match_weight=-10,
    ).as_dict()
    loaded_count = len(loaded_predictions["match_weight"])
    loaded_match_weight_sum = sum(loaded_predictions["match_weight"])

    assert loaded_count == baseline_count
    assert loaded_match_weight_sum == pytest.approx(
        baseline_match_weight_sum, rel=1e-12, abs=1e-12
    )


def test_registered_pairs_use_pruned_normal_hydration_by_default(fake_1000):
    settings = get_settings_dict()
    source_api = DuckDBAPI()
    source_linker = Linker(source_api.register(fake_1000), settings)
    blocked_pairs_arrow = (
        source_linker.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    target_api = DuckDBAPI()
    target_linker = Linker(target_api.register(fake_1000), settings)
    registered = target_api.register(blocked_pairs_arrow)
    target_linker.table_management.register_blocked_pairs_for_predict(registered)

    with patch.object(
        target_api, "_execute_sql", wraps=target_api._execute_sql
    ) as execute_sql:
        predictions = target_linker.inference.predict(threshold_match_weight=-10)

    assert "__splink__df_registered_predict_input_l" in (predictions.sql_used_to_create)
    assert "__splink__df_registered_predict_input_r" in (predictions.sql_used_to_create)
    assert "__splink__left_records as MATERIALIZED" not in (
        predictions.sql_used_to_create
    )
    pruning_sql = "\n".join(
        call.args[0]
        for call in execute_sql.call_args_list
        if "__splink__df_registered_predict_input_" in call.args[1]
    )
    assert pruning_sql.lower().count("semi join") == 2
    assert "select distinct" not in pruning_sql.lower()
    remaining_tables = {
        row[0]
        for row in target_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any(
        "__splink__df_registered_predict_input_" in name for name in remaining_tables
    )


def test_registered_pairs_allow_independent_hydration(fake_1000):
    settings = get_settings_dict()
    source_api = DuckDBAPI()
    source_linker = Linker(source_api.register(fake_1000), settings)
    blocked_pairs_arrow = (
        source_linker.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    target_api = DuckDBAPI()
    target_linker = Linker(target_api.register(fake_1000), settings)
    registered = target_api.register(blocked_pairs_arrow)
    target_linker.table_management.register_blocked_pairs_for_predict(registered)

    normal = _sort_predictions(
        target_linker.inference.predict(
            threshold_match_weight=-10,
            use_independent_hydration=False,
        )
    )
    independent_predictions = target_linker.inference.predict(
        threshold_match_weight=-10,
        use_independent_hydration=True,
    )

    assert "__splink__left_records as MATERIALIZED" in (
        independent_predictions.sql_used_to_create
    )
    assert normal == _sort_predictions(independent_predictions)


def test_registered_pair_pruning_failure_cleans_up(fake_1000):
    settings = get_settings_dict()
    source_api = DuckDBAPI()
    source_linker = Linker(source_api.register(fake_1000), settings)
    blocked_pairs_arrow = (
        source_linker.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    target_api = DuckDBAPI()
    target_linker = Linker(target_api.register(fake_1000), settings)
    registered = target_api.register(blocked_pairs_arrow)
    target_linker.table_management.register_blocked_pairs_for_predict(registered)
    original_materialize = target_linker.inference._materialize_registered_pair_input

    def fail_right_side(blocked_pairs, side):
        if side == "r":
            raise RuntimeError("injected pruning failure")
        return original_materialize(blocked_pairs, side)

    with patch.object(
        target_linker.inference,
        "_materialize_registered_pair_input",
        side_effect=fail_right_side,
    ):
        with pytest.raises(RuntimeError, match="injected pruning failure"):
            target_linker.inference.predict(threshold_match_weight=-10)

    remaining_tables = {
        row[0]
        for row in target_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any(
        "__splink__df_registered_predict_input_" in name for name in remaining_tables
    )


def test_registered_pair_pruning_supports_composite_source_ids(fake_1000):
    settings = get_settings_dict()
    settings["link_type"] = "link_only"
    df_1 = fake_1000.take(list(range(0, 1000, 2)))
    df_2 = fake_1000.take(list(range(1, 1000, 2)))

    source_api = DuckDBAPI()
    source_linker = Linker(
        [source_api.register(df_1), source_api.register(df_2)],
        settings,
    )
    expected = _sort_predictions(
        source_linker.inference.predict(threshold_match_weight=-10)
    )
    blocked_pairs_arrow = (
        source_linker.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    target_api = DuckDBAPI()
    target_linker = Linker(
        [target_api.register(df_1), target_api.register(df_2)],
        settings,
    )
    registered = target_api.register(blocked_pairs_arrow)
    target_linker.table_management.register_blocked_pairs_for_predict(registered)
    actual = _sort_predictions(
        target_linker.inference.predict(threshold_match_weight=-10)
    )

    assert expected == actual


def test_registered_pair_pruning_bypasses_near_complete_coverage():
    records = [
        {
            "unique_id": unique_id,
            "first_name": "John",
            "surname": "Smith",
            "dob": "1990-01-01",
            "email": f"person-{unique_id}@example.com",
            "city": "London",
            "cluster": unique_id,
        }
        for unique_id in range(100)
    ]
    settings = get_settings_dict()
    source_api = DuckDBAPI()
    source_linker = Linker(source_api.register(records), settings)
    blocked_pairs_arrow = (
        source_linker.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    target_api = DuckDBAPI()
    target_linker = Linker(target_api.register(records), settings)
    registered = target_api.register(blocked_pairs_arrow)
    target_linker.table_management.register_blocked_pairs_for_predict(registered)

    predictions = target_linker.inference.predict(threshold_match_weight=-10)

    assert "__splink__df_registered_predict_input_" not in (
        predictions.sql_used_to_create
    )


def test_non_duckdb_rejects_explicit_independent_hydration(fake_1000):
    settings = get_settings_dict()
    db_api = SQLiteAPI()
    linker = Linker(db_api.register(fake_1000), settings)

    with pytest.raises(SplinkException, match="supported only by DuckDB"):
        linker.inference._resolve_use_independent_hydration(
            True,
            (1, 2),
            (1, 2),
        )


def test_chunked_predict_works_in_debug_mode(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    db_api.debug_mode = True
    linker = Linker(db_api.register(fake_1000), settings)

    predictions = linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
        threshold_match_weight=-10,
    )

    assert predictions.as_duckdbpyrelation().count("*").fetchone()[0] > 0


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


def test_compute_blocked_pairs_for_predict_uses_base_key(fake_1000):
    """Test compute_blocked_pairs_for_predict() caches under the base key."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    linker.inference.compute_blocked_pairs_for_predict()

    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache


def test_blocked_pairs_not_deleted_when_from_cache(fake_1000):
    """Test that cached blocked pairs are not deleted after predict."""
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    linker = Linker(df_sdf, settings)

    linker.inference.compute_blocked_pairs_for_predict_chunk(
        left_chunk=(1, 1),
        right_chunk=(1, 1),
    )

    linker.inference.predict(threshold_match_weight=-10)

    assert "__splink__blocked_id_pairs" in linker._intermediate_table_cache


def test_register_blocked_pairs_then_predict_chunk_errors(fake_1000):
    """Test predict_chunk() and chunked predict() error after registration."""
    settings = get_settings_dict()

    db_api_source = DuckDBAPI()
    df_sdf_source = db_api_source.register(fake_1000)
    linker_source = Linker(df_sdf_source, settings)
    blocked_pairs_arrow = (
        linker_source.inference.compute_blocked_pairs_for_predict().as_pyarrow_table()
    )

    db_api_target = DuckDBAPI()
    df_sdf_target = db_api_target.register(fake_1000)
    linker_target = Linker(df_sdf_target, settings)
    blocked_pairs = db_api_target.register(blocked_pairs_arrow)
    linker_target.table_management.register_blocked_pairs_for_predict(blocked_pairs)

    # predict() with no chunk arguments must succeed and score the registered table.
    predictions = linker_target.inference.predict(threshold_match_weight=-10)
    assert len(predictions.as_dict()["match_weight"]) > 0

    # predict_chunk() is not allowed once a table is registered.
    with pytest.raises(SplinkException, match="predict\\(\\)"):
        linker_target.inference.predict_chunk(
            left_chunk=(1, 1),
            right_chunk=(1, 1),
            threshold_match_weight=-10,
        )

    # Chunked predict() is not allowed once a table is registered.
    with pytest.raises(SplinkException, match="predict"):
        linker_target.inference.predict(
            threshold_match_weight=-10,
            num_chunks_left=2,
            num_chunks_right=2,
        )


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

        assert _get_comparison_count(predictions) == baseline_count, (
            f"Chunk config ({num_left}, {num_right}) produced different count"
        )

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

        assert _get_comparison_count(predictions) == baseline_count, (
            f"Chunk config ({num_left}, {num_right}) produced different count"
        )

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

        assert _get_comparison_count(predictions) == baseline_count, (
            f"Chunk config ({num_left}, {num_right}) produced different count"
        )

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

        assert _get_comparison_count(predictions) == baseline_count, (
            f"Chunk config ({num_left}, {num_right}) produced different count"
        )

        df_chunked = _sort_predictions(predictions)
        assert df_baseline["unique_id_l"] == df_chunked["unique_id_l"]
        assert df_baseline["unique_id_r"] == df_chunked["unique_id_r"]

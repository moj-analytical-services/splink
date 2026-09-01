from unittest.mock import patch

import pytest

from splink.internals.duckdb import registered_pair_prediction
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict


def test_registered_blocked_pairs_match_from_scratch(fake_1000):
    settings = get_settings_dict()

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

    with patch.object(
        db_api_target, "_execute_sql", wraps=db_api_target._execute_sql
    ) as execute_sql:
        predictions = linker_target.inference.predict(threshold_match_weight=-10)

    assert "__splink__df_registered_predict_input_with_tf_l" in (
        predictions.sql_used_to_create
    )
    assert "__splink__df_registered_predict_input_with_tf_r" in (
        predictions.sql_used_to_create
    )
    pruning_sql = "\n".join(
        call.args[0]
        for call in execute_sql.call_args_list
        if "__splink__df_registered_predict_input_" in call.args[1]
    )
    assert pruning_sql.lower().count("semi join") == 2
    assert "select distinct" not in pruning_sql.lower()

    loaded_predictions = predictions.as_dict()
    loaded_count = len(loaded_predictions["match_weight"])
    loaded_match_weight_sum = sum(loaded_predictions["match_weight"])

    assert loaded_count == baseline_count
    assert loaded_match_weight_sum == pytest.approx(
        baseline_match_weight_sum, rel=1e-12, abs=1e-12
    )
    remaining_tables = {
        row[0]
        for row in db_api_target.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any(
        "__splink__df_registered_predict_input_" in name for name in remaining_tables
    )


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
    original_materialize = registered_pair_prediction._materialize_registered_pair_input

    def fail_right_side(linker, blocked_pairs, side):
        if side == "r":
            raise RuntimeError("injected pruning failure")
        return original_materialize(linker, blocked_pairs, side)

    with patch.object(
        registered_pair_prediction,
        "_materialize_registered_pair_input",
        side_effect=fail_right_side,
    ):
        with pytest.raises(RuntimeError, match="injected pruning failure"):
            target_linker.inference.predict(warning_mode="never")

    remaining_tables = {
        row[0]
        for row in target_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any(
        "__splink__df_registered_predict_input_" in name for name in remaining_tables
    )


def test_effective_chunks_use_registered_pair_source_pruning(fake_1000):
    settings = get_settings_dict()
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(fake_1000), settings)
    original_materialize = registered_pair_prediction._materialize_registered_pair_input

    with patch.object(
        registered_pair_prediction,
        "_materialize_registered_pair_input",
        wraps=original_materialize,
    ) as materialize:
        linker.inference.predict(warning_mode="never")
        assert materialize.call_count == 0

        linker.inference.predict_chunk(
            left_chunk=(1, 2),
            right_chunk=(1, 2),
            warning_mode="never",
        )
        assert materialize.call_count == 2

        materialize.reset_mock()
        linker.inference.predict(
            num_chunks_left=2,
            num_chunks_right=2,
            warning_mode="never",
        )
        assert materialize.call_count == 8

    remaining_tables = {
        row[0]
        for row in db_api.duckdb_con.execute(
            "select table_name from duckdb_tables()"
        ).fetchall()
    }
    assert not any(
        "__splink__df_registered_predict_input_" in name for name in remaining_tables
    )

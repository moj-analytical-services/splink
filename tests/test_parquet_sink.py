from pathlib import Path
from unittest.mock import patch

import pytest

from splink.backends.duckdb import DuckDBAPI, DuckDBAPIWithProfiling, ParquetSink
from splink.internals.linker import Linker

from .basic_settings import get_settings_dict


def _sorted_prediction_records(predictions):
    records = predictions.as_record_list()
    return sorted(
        records,
        key=lambda record: (record["unique_id_l"], record["unique_id_r"]),
    )


def _linker(fake_1000):
    db_api = DuckDBAPI()
    input_df = db_api.register(fake_1000)
    return Linker(input_df, get_settings_dict())


def _table_type(db_api, physical_name):
    return db_api.duckdb_con.sql(
        "SELECT table_type FROM information_schema.tables "
        f"WHERE table_name = '{physical_name}'"
    ).fetchone()[0]


@pytest.mark.duckdb_only
def test_predict_to_parquet_is_view_backed_and_preserves_files(fake_1000, tmp_path):
    normal_linker = _linker(fake_1000)
    normal = normal_linker.inference.predict(
        threshold_match_weight=-10, warning_mode="never"
    )

    parquet_linker = _linker(fake_1000)
    parquet_path = tmp_path / "predictions"
    parquet = parquet_linker.inference.predict(
        threshold_match_weight=-10,
        warning_mode="never",
        sink=ParquetSink(parquet_path),
    )

    assert _sorted_prediction_records(parquet) == _sorted_prediction_records(normal)
    parquet_files = list(parquet_path.glob("*.parquet"))
    assert parquet_files
    assert _table_type(parquet_linker._db_api, parquet.physical_name) == "VIEW"

    parquet.drop_table_from_database_and_remove_from_cache()
    assert all(path.is_file() for path in parquet_files)


@pytest.mark.duckdb_only
def test_predict_to_parquet_bypasses_final_prediction_materialisation(
    fake_1000, tmp_path
):
    linker = _linker(fake_1000)
    materialised_outputs = []
    original_materialiser = linker._db_api.sql_pipeline_to_splink_dataframe

    def record_materialisation(pipeline):
        materialised_outputs.append(pipeline.output_table_name)
        return original_materialiser(pipeline)

    with patch.object(
        linker._db_api,
        "sql_pipeline_to_splink_dataframe",
        side_effect=record_materialisation,
    ):
        predictions = linker.inference.predict(
            threshold_match_weight=-10,
            warning_mode="never",
            sink=ParquetSink(tmp_path / "predictions"),
        )

    assert predictions.as_record_list()
    assert "__splink__df_predict" not in materialised_outputs


@pytest.mark.duckdb_only
def test_predict_protects_existing_sink_before_blocking(fake_1000, tmp_path):
    linker = _linker(fake_1000)
    output_path = tmp_path / "predictions"
    output_path.mkdir()

    with patch.object(
        linker.inference,
        "_get_or_compute_blocked_pairs_for_predict_chunk",
        side_effect=AssertionError("blocking should not run"),
    ):
        with pytest.raises(FileExistsError, match="already exists"):
            linker.inference.predict(
                warning_mode="never",
                sink=ParquetSink(output_path),
            )


@pytest.mark.duckdb_only
def test_predict_chunk_to_parquet_matches_normal_prediction(fake_1000, tmp_path):
    normal_linker = _linker(fake_1000)
    normal = normal_linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
        threshold_match_weight=-10,
        warning_mode="never",
    )

    parquet_linker = _linker(fake_1000)
    parquet = parquet_linker.inference.predict_chunk(
        left_chunk=(1, 2),
        right_chunk=(2, 3),
        threshold_match_weight=-10,
        warning_mode="never",
        sink=ParquetSink(tmp_path / "prediction_chunk"),
    )

    assert _sorted_prediction_records(parquet) == _sorted_prediction_records(normal)


@pytest.mark.duckdb_only
def test_chunked_predict_to_parquet_combines_chunk_views(fake_1000, tmp_path):
    normal_linker = _linker(fake_1000)
    normal = normal_linker.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
        warning_mode="never",
    )

    parquet_linker = _linker(fake_1000)
    parquet_path = tmp_path / "chunked_predictions"
    parquet = parquet_linker.inference.predict(
        threshold_match_weight=-10,
        num_chunks_left=2,
        num_chunks_right=2,
        warning_mode="never",
        sink=ParquetSink(parquet_path),
    )

    assert _sorted_prediction_records(parquet) == _sorted_prediction_records(normal)
    chunk_paths = [path for path in parquet_path.iterdir() if path.is_dir()]
    assert len(chunk_paths) == 4
    assert all(list(path.glob("*.parquet")) for path in chunk_paths)
    assert _table_type(parquet_linker._db_api, parquet.physical_name) == "VIEW"


@pytest.mark.duckdb_only
def test_parquet_sink_protects_existing_output_path(tmp_path):
    output_path = Path(tmp_path) / "predictions"
    output_path.mkdir()
    (output_path / "stale.txt").write_text("stale")

    with pytest.raises(FileExistsError, match="already exists"):
        ParquetSink(output_path).prepare_root()

    ParquetSink(output_path, overwrite=True).prepare_root()
    assert output_path.is_dir()
    assert not (output_path / "stale.txt").exists()


@pytest.mark.duckdb_only
def test_empty_prediction_to_parquet_is_queryable(fake_1000, tmp_path):
    linker = _linker(fake_1000)
    predictions = linker.inference.predict(
        threshold_match_weight=1e9,
        warning_mode="never",
        sink=ParquetSink(tmp_path / "empty_predictions"),
    )

    assert predictions.as_record_list() == []
    assert predictions.columns


@pytest.mark.duckdb_only
def test_parquet_sink_rejects_non_duckdb_backend(tmp_path):
    with pytest.raises(TypeError, match="only with the DuckDB backend"):
        ParquetSink(tmp_path / "predictions").validate_db_api(object())


@pytest.mark.duckdb_only
def test_parquet_sink_rejects_debug_mode(fake_1000, tmp_path):
    linker = _linker(fake_1000)
    linker._db_api.debug_mode = True

    with pytest.raises(ValueError, match="debug_mode"):
        linker.inference.predict(
            warning_mode="never",
            sink=ParquetSink(tmp_path / "predictions"),
        )


@pytest.mark.duckdb_only
def test_parquet_sink_works_with_profiling_api(fake_1000, tmp_path):
    db_api = DuckDBAPIWithProfiling()
    input_df = db_api.register(fake_1000)
    linker = Linker(input_df, get_settings_dict())

    predictions = linker.inference.predict(
        threshold_match_weight=-10,
        warning_mode="never",
        sink=ParquetSink(tmp_path / "profiling_predictions"),
    )

    assert predictions.as_record_list()

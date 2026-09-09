import shutil
from dataclasses import FrozenInstanceError

import duckdb
import pytest

from splink import DuckDBAPI, Linker
from splink.backends.duckdb import DuckDBAPIWithProfiling, ParquetWriteOptions
from splink.internals.duckdb.dataframe import DuckDBDataFrame
from splink.internals.duckdb.parquet_materialisation import (
    _ParquetMaterialiser,
    _writer_options_sql,
)
from splink.internals.exceptions import SplinkException
from splink.internals.pipeline import CTEPipeline

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_including


@pytest.mark.parametrize(
    "kwargs",
    [
        {"compression": ""},
        {"compression": 1},
        {"compression_level": True},
        {"compression_level": "3"},
        {"per_thread_output": 1},
        {"file_size_bytes": False},
        {"file_size_bytes": 0},
        {"file_size_bytes": " "},
        {"row_group_size": -1},
        {"row_group_size": 1.5},
    ],
)
def test_invalid_options(kwargs):
    with pytest.raises((TypeError, ValueError)):
        ParquetWriteOptions(**kwargs)


def test_options_rendering_and_immutability():
    options = ParquetWriteOptions(per_thread_output=False, compression="a'b")
    with pytest.raises(FrozenInstanceError):
        options.compression = "zstd"
    assert _writer_options_sql(options) == (
        "FORMAT PARQUET, COMPRESSION 'a''b', PER_THREAD_OUTPUT FALSE"
    )
    assert _writer_options_sql(ParquetWriteOptions()) == (
        "FORMAT PARQUET, PER_THREAD_OUTPUT TRUE"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"materialisation": "unknown"},
        {"materialisation": "parquet"},
        {"materialisation_dir": "unused"},
        {"parquet_materialisation_options": ParquetWriteOptions()},
        {"materialisation": "parquet", "materialisation_dir": "s3://bucket"},
        {
            "materialisation": "parquet",
            "materialisation_dir": "unused",
            "parquet_materialisation_options": {},
        },
    ],
)
def test_validation_precedes_connections(kwargs, monkeypatch):
    def connect(*args, **kwargs):
        pytest.fail("opened a connection before validation")

    monkeypatch.setattr(duckdb, "connect", connect)
    with pytest.raises((TypeError, ValueError)):
        DuckDBAPI(**kwargs)


def test_helper_ownership(tmp_path):
    helper = _ParquetMaterialiser(tmp_path / "new", ParquetWriteOptions())
    assert not (tmp_path / "new").exists()
    sql = helper.prepare_sql("SELECT 1", "result")
    path = helper._owned_paths["result"]
    assert sql.startswith("COPY (SELECT 1)")
    assert path.is_absolute()
    assert helper.has_pending_write("result")
    assert "hive_partitioning = false" in helper.view_sql("result")
    helper.complete("result")
    assert not helper.has_pending_write("result")
    helper.delete_backing_files("untracked")
    assert path.exists()
    helper.delete_backing_files("result")
    assert not path.exists()
    helper.prepare_sql("SELECT 2", "result")
    assert helper._owned_paths["result"] != path
    shutil.rmtree(helper._owned_paths["result"])
    helper.delete_backing_files("result")
    assert not helper._owned_paths


@pytest.fixture
def api(tmp_path):
    backend = DuckDBAPI(materialisation="parquet", materialisation_dir=tmp_path)
    yield backend
    backend.delete_tables_created_by_splink_from_db()


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize(
    "options",
    [
        None,
        ParquetWriteOptions(),
        ParquetWriteOptions(per_thread_output=False),
        ParquetWriteOptions(file_size_bytes="100KB", row_group_size=2048),
        ParquetWriteOptions(
            per_thread_output=False,
            file_size_bytes=100_000,
            compression="zstd",
            compression_level=3,
            row_group_size=2048,
        ),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_layout_schema_and_direct_copy(tmp_path, options, empty, monkeypatch):
    backend = DuckDBAPI(
        materialisation="parquet",
        materialisation_dir=tmp_path / "run=example" / "it's local",
        parquet_materialisation_options=options,
    )
    statements = []
    execute = backend._execute_sql_against_backend

    def record(sql):
        statements.append(sql)
        return execute(sql)

    monkeypatch.setattr(backend, "_execute_sql_against_backend", record)
    sql = """WITH source AS (SELECT i FROM range(10000) t(i))
        SELECT i, NULL::VARCHAR AS n, [i, NULL] AS a, {'x': i} AS s,
        DATE '2024-01-01' AS d, 1.25::DECIMAL(10,2) AS money FROM source"""
    if empty:
        sql += " WHERE FALSE"
    result = backend._execute_sql(sql, "__splink__test")
    assert isinstance(result, DuckDBDataFrame)
    assert result.created_by_splink and result.sql_used_to_create == sql
    assert result.physical_name in backend._created_tables
    assert backend._intermediate_table_cache.executed_queries[-1] is result
    assert not any("CREATE TABLE" in statement for statement in statements)
    assert any(statement.startswith(f"COPY ({sql})") for statement in statements)
    assert result.as_duckdbpyrelation().types == execute(sql).types
    assert (
        result.as_duckdbpyrelation().order("i").fetchall()
        == execute(sql).order("i").fetchall()
    )
    path = backend._parquet_materialiser._owned_paths[result.physical_name]
    files = list(path.glob("*.parquet"))
    assert files
    if options and not options.per_thread_output and options.file_size_bytes is None:
        assert [p.name for p in files] == ["data.parquet"]
    if options and options.compression:
        codecs = backend.duckdb_con.sql(
            "SELECT DISTINCT compression FROM parquet_metadata(?)",
            params=[str(files[0])],
        ).fetchall()
        if not empty:
            assert codecs == [("ZSTD",)]
    backend.delete_tables_created_by_splink_from_db()
    assert not path.exists()


@mark_with_dialects_including("duckdb")
def test_ownership_replacement_exports_and_shared_root(api, tmp_path):
    other = DuckDBAPI(materialisation="parquet", materialisation_dir=tmp_path)
    other_result = other._execute_sql("SELECT 7 AS x", "other")
    unrelated = tmp_path / "unrelated.txt"
    unrelated.write_text("keep")
    registered = api.register([{"x": 1}])
    first = api._sql_to_splink_dataframe("SELECT 1 AS x", "result", "result")
    old_path = api._parquet_materialiser._owned_paths["result"]
    result = api._sql_to_splink_dataframe("SELECT 2 AS x", "result", "result")
    assert not old_path.exists()
    assert result.as_record_list() == [{"x": 2}]
    api._bind_templated_alias_to_physical("alias", "result")
    api.delete_table_from_database("alias")
    assert result.as_record_list() == [{"x": 2}]
    export = tmp_path / "export.parquet"
    first.to_parquet(str(export))
    result.created_by_splink = True
    result.drop_table_from_database_and_remove_from_cache()
    api.delete_tables_created_by_splink_from_db()
    assert export.exists() and unrelated.read_text() == "keep"
    assert registered.as_record_list() == [{"x": 1}]
    assert other_result.as_record_list() == [{"x": 7}]
    other.delete_tables_created_by_splink_from_db()


@mark_with_dialects_including("duckdb")
def test_native_mode_and_schema_connection_profiling(tmp_path):
    native = DuckDBAPI()
    assert native._materialisation == "table"
    assert native._parquet_materialiser is None
    native.query_sql("SELECT 1 AS x")
    con = duckdb.connect(str(tmp_path / "db.duckdb"))
    api = DuckDBAPIWithProfiling(
        con,
        "results",
        tmp_path / "profiles",
        materialisation="parquet",
        materialisation_dir=tmp_path / "backing",
    )
    assert api._materialisation == "parquet"
    result = api._execute_sql("SELECT 1 AS x", "result")
    assert con.sql(
        "SELECT table_type FROM information_schema.tables WHERE table_schema='results'"
    ).fetchall() == [("VIEW",)]
    profiles = list((tmp_path / "profiles").glob("*.json"))
    assert profiles and "COPY" in profiles[0].read_text()
    assert result.as_record_list() == [{"x": 1}]
    with pytest.raises(SplinkException):
        api._execute_sql("SELECT missing", "bad")
    assert not api._profiling_active
    assert len(api._parquet_materialiser._pending_queries) == 1
    failed_name = next(iter(api._parquet_materialiser._pending_queries))
    api.delete_table_from_database(failed_name)
    api.delete_tables_created_by_splink_from_db()


@mark_with_dialects_including("duckdb")
def test_debug_pipeline(api):
    api.debug_mode = True
    pipeline = CTEPipeline()
    pipeline.enqueue_sql("SELECT 1 AS x", "__splink__first")
    pipeline.enqueue_sql("SELECT x + 1 AS x FROM __splink__first", "__splink__last")
    result = api.sql_pipeline_to_splink_dataframe(pipeline)
    assert result.as_record_list() == [{"x": 2}]
    assert set(api._parquet_materialiser._owned_paths) == {result.physical_name}
    assert not api.table_exists_in_database("__splink__first")


@mark_with_dialects_including("duckdb")
def test_prediction_cached_blocking_and_chunks(api, fake_1000):
    settings = get_settings_dict()
    native = DuckDBAPI()
    baseline = Linker(native.register(fake_1000), settings).inference.predict()

    def canonical(result):
        return (
            result.as_duckdbpyrelation()
            .project("unique_id_l, unique_id_r, match_weight")
            .order("unique_id_l, unique_id_r")
            .fetchall()
        )

    expected = canonical(baseline)
    linker = Linker(api.register(fake_1000), settings)
    for cached, chunks in [(False, 1), (True, 1), (False, 2), (True, 2)]:
        linker.table_management.invalidate_cache()
        if cached:
            for left in range(1, chunks + 1):
                for right in range(1, chunks + 1):
                    linker.inference.compute_blocked_pairs_for_predict_chunk(
                        left_chunk=(left, chunks), right_chunk=(right, chunks)
                    )
        result = linker.inference.predict(
            num_chunks_left=chunks, num_chunks_right=chunks
        )
        actual = canonical(result)
        assert [row[:2] for row in actual] == [row[:2] for row in expected]
        assert [w for _, _, w in actual] == pytest.approx([w for _, _, w in expected])
        assert not api._parquet_materialiser._pending_queries


@mark_with_dialects_including("duckdb")
@pytest.mark.parametrize("parallel", [False, True])
def test_file_rollover(tmp_path, parallel):
    api = DuckDBAPI(
        materialisation="parquet",
        materialisation_dir=tmp_path,
        parquet_materialisation_options=ParquetWriteOptions(
            per_thread_output=parallel,
            file_size_bytes="100KB",
            row_group_size=2048,
            compression="uncompressed",
        ),
    )
    # One writer thread demonstrates rollover rather than thread count.
    api.duckdb_con.execute("SET threads=1")
    result = api._execute_sql(
        "SELECT i, md5(i::VARCHAR) AS text FROM range(100000) t(i)", "rollover"
    )
    path = api._parquet_materialiser._owned_paths[result.physical_name]
    assert len(list(path.glob("*.parquet"))) > 1
    assert result.as_duckdbpyrelation().count("*").fetchone() == (100000,)
    api.delete_tables_created_by_splink_from_db()


@mark_with_dialects_including("duckdb")
def test_invalid_writer_setting_is_not_ignored(api):
    api._parquet_materialiser._write_options = ParquetWriteOptions(
        compression="not_a_codec"
    )
    with pytest.raises(SplinkException, match="not_a_codec"):
        api._execute_sql("SELECT 1 AS x", "invalid_codec")
    assert len(api._parquet_materialiser._owned_paths) == 1
    failed_name = next(iter(api._parquet_materialiser._owned_paths))
    api.delete_table_from_database(failed_name)
    assert not api._parquet_materialiser._owned_paths

from __future__ import annotations

import pytest

import splink.comparison_library as cl
from splink import Linker, SettingsCreator
from splink.backends.duckdb import (
    DuckDBAPI,
    DuckDBAPIWithParquetMaterialisation,
)


def _table_type(db_api, physical_name):
    return db_api.duckdb_con.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [physical_name],
    ).fetchone()


def _prediction_settings():
    return SettingsCreator(
        link_type="dedupe_only",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=["l.first_name = r.first_name"],
        probability_two_random_records_match=0.1,
    )


def _make_linker(db_api):
    records = [
        {"unique_id": 1, "first_name": "Ada"},
        {"unique_id": 2, "first_name": "Ada"},
        {"unique_id": 3, "first_name": "Grace"},
    ]
    return Linker(db_api.register(records), _prediction_settings())


def _run_prediction(db_api):
    linker = _make_linker(db_api)
    predictions = linker.inference.predict()
    return linker, predictions


@pytest.mark.duckdb_only
def test_basic_materialisation_is_a_parquet_backed_view(tmp_path):
    db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )

    dataframe = db_api.query_sql("select 1 as x")
    parquet_path = db_api._parquet_path_for_physical_name(dataframe.physical_name)

    assert dataframe.as_record_list() == [{"x": 1}]
    assert _table_type(db_api, dataframe.physical_name) == ("VIEW",)
    assert parquet_path.is_dir()

    assert list(parquet_path.glob("*.parquet"))


@pytest.mark.duckdb_only
def test_drop_removes_view_and_parquet_data(tmp_path):
    db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )
    dataframe = db_api.query_sql("select 1 as x")
    parquet_path = db_api._parquet_path_for_physical_name(dataframe.physical_name)

    dataframe.drop_table_from_database_and_remove_from_cache()

    assert _table_type(db_api, dataframe.physical_name) is None
    assert not parquet_path.exists()


@pytest.mark.duckdb_only
def test_prediction_matches_duckdb_and_is_parquet_backed(tmp_path):
    normal_linker, normal_predictions = _run_prediction(DuckDBAPI())
    parquet_db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )
    parquet_linker, parquet_predictions = _run_prediction(parquet_db_api)

    assert sorted(normal_predictions.as_record_list()) == sorted(
        parquet_predictions.as_record_list()
    )
    assert _table_type(parquet_db_api, parquet_predictions.physical_name) == ("VIEW",)
    assert list(
        parquet_db_api._parquet_path_for_physical_name(
            parquet_predictions.physical_name
        ).glob("*.parquet")
    )
    assert not any(
        path.name.startswith("__splink__blocked_id_pairs")
        for path in parquet_db_api.parquet_materialisation_dir.iterdir()
    )

    normal_linker._db_api.delete_tables_created_by_splink_from_db()
    parquet_linker._db_api.delete_tables_created_by_splink_from_db()


@pytest.mark.duckdb_only
def test_blocked_pairs_are_parquet_backed(tmp_path):
    parquet_db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )
    linker, _ = _run_prediction(parquet_db_api)

    blocked_pairs = linker.inference.compute_blocked_pairs_for_predict()
    parquet_path = parquet_db_api._parquet_path_for_physical_name(
        blocked_pairs.physical_name
    )

    assert _table_type(parquet_db_api, blocked_pairs.physical_name) == ("VIEW",)
    assert parquet_path.is_dir()
    assert list(parquet_path.glob("*.parquet"))


@pytest.mark.duckdb_only
def test_empty_materialisation_preserves_columns(tmp_path):
    db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )

    dataframe = db_api.query_sql("select cast(null as integer) as x where false")

    assert dataframe.as_record_list() == []
    assert dataframe.as_duckdbpyrelation().columns == ["x"]
    assert _table_type(db_api, dataframe.physical_name) == ("VIEW",)


@pytest.mark.duckdb_only
def test_debug_mode_materialisations_are_cleaned_up(tmp_path):
    db_api = DuckDBAPIWithParquetMaterialisation(
        parquet_materialisation_dir=tmp_path / "materialisations"
    )
    linker = _make_linker(db_api)
    linker._debug_mode = True

    dataframe = linker.inference.predict()
    parquet_path = db_api._parquet_path_for_physical_name(dataframe.physical_name)

    assert dataframe.as_record_list()
    assert _table_type(db_api, dataframe.physical_name) == ("VIEW",)
    assert parquet_path.is_dir()

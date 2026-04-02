import os
from unittest.mock import create_autospec, patch

import pandas as pd
import pytest

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.duckdb.dataframe import DuckDBDataFrame
from splink.internals.linker import Linker, SplinkDataFrame
from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


def make_mock_execute(db_api):
    dummy_table_name = "__splink__dummy_frame"

    db_api._execute_sql_against_backend(
        f"CREATE TABLE IF NOT EXISTS {dummy_table_name} AS SELECT 1 AS id"
    )

    def register_and_return_dummy_frame(
        sql, templated_name, physical_name, *args, **kwargs
    ):
        db_api._execute_sql_against_backend(
            f"""
            CREATE TABLE IF NOT EXISTS {physical_name} AS
            SELECT * FROM {dummy_table_name}
            """
        )

        return DuckDBDataFrame(templated_name, physical_name, db_api)

    return create_autospec(
        db_api._sql_to_splink_dataframe, side_effect=register_and_return_dummy_frame
    )


def test_cache_id(tmp_path):
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, get_settings_dict())

    prior = linker._settings_obj._cache_uid

    path = os.path.join(tmp_path, "model.json")
    linker.misc.save_model_to_json(path, overwrite=True)

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker_2 = Linker(df_sdf, path)

    assert linker_2._settings_obj._cache_uid == prior

    random_uid = "my_random_uid"
    settings = get_settings_dict()
    settings["linker_uid"] = random_uid
    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker_uid = linker._cache_uid
    assert linker_uid == random_uid


def test_cache_only_splink_dataframes():
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker._intermediate_table_cache["new_table"] = DuckDBDataFrame(
        "template", "__splink__dummy_frame", db_api
    )
    try:
        linker._intermediate_table_cache["not_a_table"] = 30
    except TypeError:
        pass
    for _, table in linker._intermediate_table_cache.items():
        assert isinstance(table, SplinkDataFrame)


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_access_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_invalidate_tf_tables(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker._debug_mode = debug_mode

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_not_called()

        linker.table_management.invalidate_cache()

        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_invalidates_with_new_linker_for_tf_tables(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()

    db_api = DuckDBAPI()
    df_sdf_new = db_api.register(df)

    new_linker = Linker(df_sdf_new, settings)
    new_linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        new_linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        new_linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_register_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)

    linker = Linker(df_sdf, settings)
    linker._debug_mode = debug_mode

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.register_term_frequency_lookup(df, "first_name")
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()

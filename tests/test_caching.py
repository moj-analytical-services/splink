import os
from unittest.mock import create_autospec, patch

import pandas as pd
import pytest

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.duckdb.dataframe import DuckDBDataFrame
from splink.internals.linker import Linker, SplinkDataFrame
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import (
    compute_df_concat,
    compute_df_concat_with_tf,
    enqueue_df_concat_with_tf,
)
from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
_dummy_pd_frame = pd.DataFrame(["id"])


def make_mock_execute(db_api):
    # creates a mock version of linker._sql_to_splink_dataframe,
    # so we can count calls
    dummy_table_name = "__splink__dummy_frame"

    # ensure a tiny seed table exists to copy from
    db_api._execute_sql_against_backend(
        f"CREATE TABLE IF NOT EXISTS {dummy_table_name} AS SELECT 1 AS id"
    )

    def register_and_return_dummy_frame(
        sql, templated_name, physical_name, *args, **kwargs
    ):
        # Make sure the physical target exists so that:
        #  - caching can find it, and
        #  - debug overlays can bind a view to it safely.
        db_api._execute_sql_against_backend(
            f"""
            CREATE TABLE IF NOT EXISTS {physical_name} AS
            SELECT * FROM {dummy_table_name}
            """
        )

        # Return a SplinkDataFrame that reflects the real names for the cache
        return DuckDBDataFrame(templated_name, physical_name, db_api)

    mock_execute = create_autospec(
        db_api._sql_to_splink_dataframe, side_effect=register_and_return_dummy_frame
    )
    return mock_execute


def test_cache_id(tmp_path):
    # Test saving and loading a model
    db_api = DuckDBAPI()

    linker = Linker(df, get_settings_dict(), db_api=db_api)

    prior = linker._settings_obj._cache_uid

    path = os.path.join(tmp_path, "model.json")
    linker.misc.save_model_to_json(path, overwrite=True)

    db_api = DuckDBAPI()

    linker_2 = Linker(df, path, db_api=db_api)

    assert linker_2._settings_obj._cache_uid == prior

    # Test uid from settings
    random_uid = "my_random_uid"
    settings = get_settings_dict()
    settings["linker_uid"] = random_uid
    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker_uid = linker._cache_uid
    assert linker_uid == random_uid


def test_cache_only_splink_dataframes():
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._intermediate_table_cache["new_table"] = DuckDBDataFrame(
        "template", "__splink__dummy_frame", linker
    )
    try:
        linker._intermediate_table_cache["not_a_table"] = 30
    except TypeError:
        # error is raised, but need to check it hasn't made it to the cache
        pass
    for _, table in linker._intermediate_table_cache.items():
        assert isinstance(table, SplinkDataFrame)


# run test in/not in debug mode to check functionality in both - cache shouldn't care
@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_access_df_concat(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        # shouldn't touch DB if we don't materialise
        pipeline = CTEPipeline()
        pipeline = enqueue_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()

        # this should create the table in the db
        compute_df_concat_with_tf(linker, pipeline)
        # NB don't specify amount of times it is called, as will depend on debug_mode
        mockexecute_sql_pipeline.assert_called()
        # reset the call counter on the mock
        mockexecute_sql_pipeline.reset_mock()

        # this should NOT touch the database, but instead use the cache
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()

        # this should also use the cache - concat will just refer to concat_with_tf
        compute_df_concat(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_access_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_called()
        # reset the call counter on the mock
        mockexecute_sql_pipeline.reset_mock()

        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_invalidate_cache(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        # this should NOT touch the database, but instead use the cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()

        # create this:
        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()
        # then check the cache
        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_not_called()

        linker.table_management.invalidate_cache()

        # now we _SHOULD_ compute afresh:
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()
        # but now draw from the cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()
        # and should compute this again:
        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()
        # then check the cache
        linker.table_management.compute_tf_table("surname")
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_invalidates_with_new_linker(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        # should use cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()

    db_api = DuckDBAPI()

    new_linker = Linker(df, settings, db_api=db_api)
    new_linker._debug_mode = debug_mode
    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        # new linker should recalculate df_concat_with_tf
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(new_linker, pipeline)
        mockexecute_sql_pipeline.assert_called()
        mockexecute_sql_pipeline.reset_mock()

        # but now read from the cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(new_linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        # original linker should still have result cached
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_register_compute_concat_with_tf_table(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        # can actually register frame, as that part not cached
        # don't need function so use any frame
        linker.table_management.register_table_input_nodes_concat_with_tf(df)
        # now this should be cached, as I have manually registered
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(linker, pipeline)
        mockexecute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_register_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    db_api = DuckDBAPI()

    linker = Linker(df, settings, db_api=db_api)
    linker._debug_mode = debug_mode

    with patch.object(
        db_api, "_sql_to_splink_dataframe", new=make_mock_execute(db_api)
    ) as mockexecute_sql_pipeline:
        # can actually register frame, as that part not cached
        # don't need function so use any frame
        linker.table_management.register_term_frequency_lookup(df, "first_name")
        # now this should be cached, as I have manually registered
        linker.table_management.compute_tf_table("first_name")
        mockexecute_sql_pipeline.assert_not_called()

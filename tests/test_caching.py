import os
from unittest.mock import create_autospec, patch

import pandas as pd
import pytest

from splink.duckdb.linker import DuckDBLinker, DuckDBLinkerDataFrame
from splink.linker import SplinkDataFrame
from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
__splink__dummy_frame = pd.DataFrame(["id"])


def make_mock_execute(linker):
    # creates a mock version of linker._execute_sql_against_backend,
    # so we can count calls
    dummy_splink_df = DuckDBLinkerDataFrame("template", "__splink__dummy_frame", linker)
    mock_execute = create_autospec(
        linker._execute_sql_against_backend, return_value=dummy_splink_df
    )
    return mock_execute


def test_cache_id(tmp_path):
    # Test saving and loading a model
    linker = DuckDBLinker(
        df,
        get_settings_dict(),
    )

    prior = linker._settings_obj._cache_uid

    path = os.path.join(tmp_path, "model.json")
    linker.save_model_to_json(path, overwrite=True)

    linker_2 = DuckDBLinker(df)
    linker_2.load_model(path)

    assert linker_2._settings_obj._cache_uid == prior

    # Test initialising settings
    linker = DuckDBLinker(
        df,
    )
    prior = linker._cache_uid

    linker.load_settings(get_settings_dict())
    assert prior == linker._cache_uid

    # Test uid from settings
    random_uid = "my_random_uid"
    settings = get_settings_dict()
    settings["linker_uid"] = random_uid
    linker = DuckDBLinker(df, settings)
    linker_uid = linker._cache_uid
    assert linker_uid == random_uid


def test_materialising_works():
    # A quick check to ensure pipelining and materialising
    # works as expected across our concat and tf tables.

    # As these tables are all intertwined and depend on one another,
    # we need to ensure we don't end up with any circular CTE expressions.

    # The pipeline should now be reset if `materialise` is called.

    settings = get_settings_dict()

    # Train from label column
    linker = DuckDBLinker(df, settings)

    linker._initialise_df_concat(materialise=False)
    linker._initialise_df_concat_with_tf(materialise=True)

    linker = DuckDBLinker(df, settings)
    linker._initialise_df_concat_with_tf(materialise=False)
    linker._initialise_df_concat(materialise=True)
    linker.compute_tf_table("first_name")

    linker = DuckDBLinker(df, settings)
    linker._initialise_df_concat_with_tf(materialise=False)
    linker.compute_tf_table("first_name")

    linker = DuckDBLinker(df, settings)
    linker._initialise_df_concat_with_tf(materialise=True)
    linker.compute_tf_table("first_name")


def test_cache_only_splink_dataframes():
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker._intermediate_table_cache["new_table"] = DuckDBLinkerDataFrame(
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
def test_cache_access_initialise_df_concat(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode
    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        # shouldn't touch DB if we don't materialise
        linker._initialise_df_concat_with_tf(materialise=False)
        mock_execute_sql_pipeline.assert_not_called()

        # this should create the table in the db
        linker._initialise_df_concat_with_tf(materialise=True)
        # NB don't specify amount of times it is called, as will depend on debug_mode
        mock_execute_sql_pipeline.assert_called()
        # reset the call counter on the mock
        mock_execute_sql_pipeline.reset_mock()

        # this should NOT touch the database, but instead use the cache
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()

        # this should also use the cache - concat will just refer to concat_with_tf
        linker._initialise_df_concat(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_access_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode
    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        linker.compute_tf_table("first_name")
        mock_execute_sql_pipeline.assert_called()
        # reset the call counter on the mock
        mock_execute_sql_pipeline.reset_mock()

        linker.compute_tf_table("first_name")
        mock_execute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_invalidate_cache(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode

    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()

        # this should NOT touch the database, but instead use the cache
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()

        # create this:
        linker.compute_tf_table("surname")
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()
        # then check the cache
        linker.compute_tf_table("surname")
        mock_execute_sql_pipeline.assert_not_called()

        linker.invalidate_cache()

        # now we _SHOULD_ compute afresh:
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()
        # but now draw from the cache
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()
        # and should compute this again:
        linker.compute_tf_table("surname")
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()
        # then check the cache
        linker.compute_tf_table("surname")
        mock_execute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_invalidates_with_new_linker(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode
    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()

        # should use cache
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()

    new_linker = DuckDBLinker(df, settings)
    new_linker.debug_mode = debug_mode
    with patch.object(
        new_linker, "_execute_sql_against_backend", new=make_mock_execute(new_linker)
    ) as mock_execute_sql_pipeline:
        # new linker should recalculate df_concat_with_tf
        new_linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_called()
        mock_execute_sql_pipeline.reset_mock()

        # but now read from the cache
        new_linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()

    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        # original linker should still have result cached
        linker._initialise_df_concat_with_tf(materialise=True)
        mock_execute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_register_compute_concat_with_tf_table(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode

    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        # can actually register frame, as that part not cached
        # don't need function so use any frame
        linker.register_table_input_nodes_concat_with_tf(df)
        # now this should be cached, as I have manually registered
        linker._initialise_df_concat_with_tf()
        mock_execute_sql_pipeline.assert_not_called()


@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_register_compute_tf_table(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode

    with patch.object(
        linker, "_execute_sql_against_backend", new=make_mock_execute(linker)
    ) as mock_execute_sql_pipeline:
        # can actually register frame, as that part not cached
        # don't need function so use any frame
        linker.register_term_frequency_lookup(df, "first_name")
        # now this should be cached, as I have manually registered
        linker.compute_tf_table("first_name")
        mock_execute_sql_pipeline.assert_not_called()

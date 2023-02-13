import os
from unittest.mock import patch, create_autospec

import pandas as pd
import pytest

from splink.duckdb.duckdb_linker import DuckDBLinker, DuckDBLinkerDataFrame
from splink.linker import SplinkDataFrame

from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
dummy_frame = pd.DataFrame(["id"])


def make_mock_execute(linker):
    # creates a mock version of linker._execute_sql_against_backend,
    # so we can count calls
    dummy_splink_df = DuckDBLinkerDataFrame("template", "dummy_frame", linker)
    mock_execute = create_autospec(
        linker._execute_sql_against_backend,
        return_value=dummy_splink_df
    )
    return mock_execute


def test_cache_id(tmp_path):

    # Test saving and loading from settings
    linker = DuckDBLinker(
        df,
        get_settings_dict(),
    )

    prior = linker._settings_obj._cache_uuid

    path = os.path.join(tmp_path, "model.json")
    linker.save_settings_to_json(path, overwrite=True)

    linker_2 = DuckDBLinker(df, connection=":memory:")
    linker_2.load_settings_from_json(path)

    assert linker_2._settings_obj._cache_uuid == prior

    # Test initialising settings
    linker = DuckDBLinker(
        df,
    )
    prior = linker._cache_uuid

    linker.initialise_settings(get_settings_dict())
    assert prior == linker._cache_uuid

    # Test uuid from settings
    random_uuid = "my_random_uuid"
    settings = get_settings_dict()
    settings["linker_uuid"] = random_uuid
    linker = DuckDBLinker(df, settings)
    linker_uuid = linker._cache_uuid
    assert linker_uuid == random_uuid


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


# run test in/not in debug mode to check functionality in both - cache shouldn't care
@pytest.mark.parametrize("debug_mode", (False, True))
def test_cache_access_initialise_df_concat(debug_mode):
    settings = get_settings_dict()

    linker = DuckDBLinker(df, settings)
    linker.debug_mode = debug_mode
    with patch.object(
            linker,
            "_execute_sql_against_backend",
            new=make_mock_execute(linker)
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
            linker,
            "_execute_sql_against_backend",
            new=make_mock_execute(linker)
            ) as mock_execute_sql_pipeline:
        linker.compute_tf_table("first_name")
        mock_execute_sql_pipeline.assert_called()
        # reset the call counter on the mock
        mock_execute_sql_pipeline.reset_mock()

        linker.compute_tf_table("first_name")
        mock_execute_sql_pipeline.assert_not_called()

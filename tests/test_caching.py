import os
from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd

from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")


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
    linker = DuckDBLinker(
        df,
        settings
    )
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

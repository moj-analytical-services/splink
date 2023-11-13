import pandas as pd
import pytest

from splink.duckdb.linker import DuckDBLinker
from splink.exceptions import SplinkException
from tests.basic_settings import get_settings_dict

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
df_l = df.copy()
df_r = df.copy()
df_l["source_dataset"] = "my_left_ds"
df_r["source_dataset"] = "my_right_ds"
df_final = pd.concat([df_l, df_r])

settings = get_settings_dict()
settings["link_type"] = "link_only"


# def test_link_only_verification():
#     # As `_initialise_df_concat_with_tf()` cannot be run without
#     # a setting object, we don't need to test that.

#     # Two input dataframes + link only settings
#     linker = DuckDBLinker(
#         [df_l, df_r],
#         settings,
#     )
#     linker._initialise_df_concat_with_tf()

#     # A single dataframe with a source_dataset col
#     linker = DuckDBLinker(
#         df_final,
#         settings,
#     )
#     linker._initialise_df_concat_with_tf()

#     # A single df with no source_dataset col, despite
#     # calling link_only. Should fail w/ SplinkException
#     linker = DuckDBLinker(
#         df,
#         settings,
#     )
#     # This should pass as concat_with_tf doesn't yet exist
#     linker._verify_link_only_job
#     with pytest.raises(SplinkException):
#         # Fails as only one df w/ no source_dataset col has
#         # been passed
#         linker._initialise_df_concat_with_tf()

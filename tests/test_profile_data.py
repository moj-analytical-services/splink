from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd
from basic_settings import settings_dict
from splink.profile_data import column_frequency_chart


def test_profile_using_duckdb():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = DuckDBLinker(
        settings_dict, input_tables={"fake_data_1": df}, connection=":memory:"
    )

    linker.column_frequency_chart(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )

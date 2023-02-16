import sqlite3
import pandas as pd

from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.sqlite.sqlite_linker import SQLiteLinker
from splink.spark.spark_linker import SparkLinker
from splink.misc import ensure_is_list
from splink.profile_data import (
    _col_or_expr_frequencies_raw_data_sql,
)

from basic_settings import get_settings_dict


def generate_raw_profile_dataset(columns_to_profile):
    # Grabs the underlying data created to form our profile_columns charts

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df["blank"] = None
    settings_dict = get_settings_dict()
    linker = DuckDBLinker(df, settings_dict, connection=":memory:")

    input_tablename = "__splink__df_concat_with_tf"
    if not linker._table_exists_in_database("__splink__df_concat_with_tf"):
        linker._initialise_df_concat()
        input_tablename = "__splink__df_concat"

    column_expressions_raw = ensure_is_list(columns_to_profile)

    sql = _col_or_expr_frequencies_raw_data_sql(
        column_expressions_raw,
        input_tablename
    )

    linker._enqueue_sql(sql, "__splink__df_all_column_value_frequencies")
    return linker._execute_sql_pipeline(materialise_as_hash=True).as_pandas_dataframe()


def test_profile_using_duckdb():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings_dict = get_settings_dict()
    linker = DuckDBLinker(df, settings_dict, connection=":memory:")

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=5,
    )

    linker.profile_columns(
        ["first_name", "surname", ["first_name", "surname"], ["city", "first_name"]],
        top_n=15,
        bottom_n=5,
    )

    assert len(generate_raw_profile_dataset([["first_name", "blank"]])) == 0


def test_profile_using_duckdb_no_settings():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = DuckDBLinker(df, connection=":memory:")

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=5,
    )


def test_profile_using_sqlite():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    con = sqlite3.connect(":memory:")

    df.to_sql("fake_data_1", con, if_exists="replace")
    settings_dict = get_settings_dict()
    linker = SQLiteLinker(
        "fake_data_1",
        settings_dict,
        connection=con,
    )

    linker.profile_columns(["first_name", "surname", "first_name || surname"])


# @pytest.mark.skip(reason="Uses Spark so slow and heavyweight")
def test_profile_using_spark(df_spark):
    settings_dict = get_settings_dict()
    linker = SparkLinker(df_spark, settings_dict)

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )

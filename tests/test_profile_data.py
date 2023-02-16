import sqlite3
import pandas as pd
import numpy as np

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


def test_profile_with_arrays_duckdb():
    dic = {
        "id": {0: 1, 1: 2, 2: 3, 3: 4},
        "forename": {0: "Juan", 1: "Sarah", 2: "Leila", 3: "Michaela"},
        "surname": {0: "Pene", 1: "Dowel", 2: "Idin", 3: "Bose"},
        "offence_code_arr": {
            0: np.nan,
            1: np.array((1, 2, 3)),
            2: np.array((1, 2, 3)),
            3: np.array((1, 2, 3)),
        },
        "lat_long": {
            0: {"lat": 22.730590, "lon": 9.388589},
            1: {"lat": 22.836322, "lon": 9.276112},
            2: {"lat": 37.770850, "lon": 95.689880},
            3: None,
        },
    }

    df = pd.DataFrame(dic)
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "id",
    }
    linker = DuckDBLinker(df, settings, connection=":memory:")

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    linker.profile_columns(
        column_expressions,
        top_n=3,
        bottom_n=3,
    )


def test_profile_with_arrays_spark(spark):
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "id",
    }
    spark_df = spark.read.parquet("tests/datasets/arrays_df.parquet")
    spark_df.persist()

    linker = SparkLinker(
        spark_df,
        settings,
    )

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    linker.profile_columns(
        column_expressions,
        top_n=3,
        bottom_n=3,
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

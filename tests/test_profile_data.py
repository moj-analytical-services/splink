import os
import sqlite3

import numpy as np
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from splink.duckdb.linker import DuckDBLinker
from splink.misc import ensure_is_list
from splink.profile_data import (
    _col_or_expr_frequencies_raw_data_sql,
)
from splink.spark.linker import SparkLinker
from splink.sqlite.linker import SQLiteLinker
from tests.decorator import mark_with_dialects_excluding

from .basic_settings import get_settings_dict


def generate_raw_profile_arrays_dataset(columns_to_profile, linker, cast_arrays_as_str):

    df_concat = linker._initialise_df_concat()

    array_cols = df_concat.get_array_cols()

    column_expressions_raw = ensure_is_list(columns_to_profile)

    sql = _col_or_expr_frequencies_raw_data_sql(
        column_expressions_raw, array_cols, df_concat.physical_name, cast_arrays_as_str
    )

    linker._enqueue_sql(sql, "__splink__df_all_column_value_frequencies")

    return linker._execute_sql_pipeline().as_pandas_dataframe()


def test_profile_using_duckdb():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df["blank"] = None
    settings_dict = get_settings_dict()

    linker = DuckDBLinker(df, settings_dict, connection=":memory:")

    bools = [False, True]

    for bool in bools:

        linker.profile_columns(
            [
                "first_name",
                "surname",
                "first_name || surname",
                "concat(city, first_name)",
            ],
            top_n=15,
            bottom_n=15,
            cast_arrays_as_str=bool,
        )
        linker.profile_columns(
            [
                "first_name",
                ["surname"],
                ["first_name", "surname"],
                ["city", "first_name", "dob"],
                ["first_name", "surname", "city", "dob"],
            ],
            top_n=15,
            bottom_n=15,
            cast_arrays_as_str=bool,
        )

        assert (
            len(
                generate_raw_profile_arrays_dataset(
                    [["first_name", "blank"]], linker, cast_arrays_as_str=bool
                )
            )
            == 0
        )


def test_profile_using_duckdb_no_settings():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df["blank"] = None

    linker = DuckDBLinker(df, connection=":memory:")

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    linker.profile_columns(
        [
            "first_name",
            ["surname"],
            ["first_name", "surname"],
            ["city", "first_name", "dob"],
            ["first_name", "surname", "city", "dob"],
        ],
        top_n=15,
        bottom_n=15,
    )

    assert (
        len(
            generate_raw_profile_arrays_dataset(
                [["first_name", "blank"]], linker, cast_arrays_as_str=True
            )
        )
        == 0
    )


# @pytest.mark.skip(reason="Uses Spark so slow and heavyweight")
def test_profile_using_spark(df_spark):
    settings_dict = get_settings_dict()
    df_spark = df_spark.withColumn("blank", lit(None).cast(StringType()))
    linker = SparkLinker(df_spark, settings_dict)

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    linker.profile_columns(
        [
            "first_name",
            ["surname"],
            ["first_name", "surname"],
            ["city", "first_name", "dob"],
            ["first_name", "surname", "city", "dob"],
        ],
        top_n=15,
        bottom_n=15,
    )

    assert (
        len(
            generate_raw_profile_arrays_dataset(
                [["first_name", "blank"]], linker, cast_arrays_as_str=True
            )
        )
        == 0
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


@mark_with_dialects_excluding("sqlite", "spark")
def test_profile_arrays_bat(test_helpers, dialect, tmp_path):
    helper = test_helpers[dialect]

    dic = {
        "id": {0: 1, 1: 2, 2: 3, 3: 4},
        "forename": {0: "Juan", 1: "Sarah", 2: "Leila", 3: "Michaela"},
        "surname": {0: "Pene", 1: "Dowel", 2: "Idin", 3: "Bose"},
        "offence_code_arr": {
            0: np.nan,
            1: np.array((11, 12, 13, 13)),
            2: np.array((12, 13)),
            3: np.array((11, 12, 13)),
        },
        "lat_long": {
            0: {"lat": 22.730590, "lon": 9.388589},
            1: {"lat": 22.836322, "lon": 9.276112},
            2: {"lat": 37.770850, "lon": 95.689880},
            3: None,
        },
    }

    df = pd.DataFrame(dic)
    df["blank"] = None

    # Writing out and reading in as parquet as the pandas dfs were causing issues
    r_w_path = os.path.join(tmp_path, "helper_df")

    df.to_parquet(r_w_path)

    df = helper.load_frame_from_parquet(r_w_path)

    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "id",
    }
    linker = helper.Linker(df, settings, **helper.extra_linker_args())

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    bools = [False, True]

    for bool in bools:

        linker.profile_columns(
            column_expressions,
            top_n=3,
            bottom_n=3,
        )

        linker.profile_columns(
            [
                "forename",
                ["surname"],
                ["forename", "surname"],
            ],
            top_n=3,
            bottom_n=3,
        )

        assert (
            len(
                generate_raw_profile_arrays_dataset(
                    [["forename", "blank"]], linker, cast_arrays_as_str=bool
                )
            )
            == 0
        )

        out = generate_raw_profile_arrays_dataset(
            "offence_code_arr", linker, cast_arrays_as_str=bool
        )

        if bool is False:

            expected = {"value": [13, 12, 11], "value_count": [4, 3, 2]}

            out["value"] = out["value"].astype("int")

        else:

            expected = {
                "value": ["[12, 13]", "[11, 12, 13]", "[11, 12, 13, 13]"],
                "value_count": [1, 1, 1],
            }

        expected = pd.DataFrame.from_dict(expected)

        out = out.loc[:, ["value", "value_count"]].sort_values(
            by="value", ascending=False, ignore_index=True
        )

        assert expected.equals(out)


def test_profile_with_arrays_spark(spark):
    settings = {
        "link_type": "dedupe_only",
        "unique_id_column_name": "id",
    }
    spark_df = spark.read.parquet("tests/datasets/arrays_df.parquet")
    spark_df = spark_df.withColumn("blank", lit(None).cast(StringType()))
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

    assert (
        len(
            generate_raw_profile_arrays_dataset(
                [["forename", "blank"]], linker, cast_arrays_as_str=False
            )
        )
        == 0
    )

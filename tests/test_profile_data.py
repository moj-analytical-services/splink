import sqlite3

import numpy as np
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.misc import ensure_is_list
from splink.internals.pipeline import CTEPipeline
from splink.internals.profile_data import (
    _col_or_expr_frequencies_raw_data_sql,
    profile_columns,
)
from splink.internals.sqlite.database_api import SQLiteAPI

from .decorator import mark_with_dialects_including


def generate_raw_profile_dataset(table, columns_to_profile, db_api):
    input_alias = "__splink__profile_data"
    _splink_df = db_api.register_table(table, input_alias, overwrite=True)

    pipeline = CTEPipeline()

    column_expressions_raw = ensure_is_list(columns_to_profile)

    sql = _col_or_expr_frequencies_raw_data_sql(column_expressions_raw, input_alias)

    pipeline.enqueue_sql(sql, "__splink__df_all_column_value_frequencies")

    return db_api.sql_pipeline_to_splink_dataframe(pipeline).as_pandas_dataframe()


@mark_with_dialects_including("duckdb")
def test_profile_default_cols_duckdb():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    db_api = DuckDBAPI()

    profile_columns(
        df,
        db_api,
    )


@mark_with_dialects_including("duckdb")
def test_profile_using_duckdb():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df["blank"] = None
    db_api = DuckDBAPI(connection=":memory:")

    profile_columns(
        df,
        db_api,
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    profile_columns(
        df,
        db_api,
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

    assert len(generate_raw_profile_dataset(df, [["first_name", "blank"]], db_api)) == 0


# probably dropping support for this, so won't fixup
# def test_profile_using_duckdb_no_settings():
#     df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

#     linker = DuckDBLinker(df, connection=":memory:")

#     linker.profile_columns(
#         ["first_name", "surname",
#               "first_name || surname", "concat(city, first_name)"],
#         top_n=15,
#         bottom_n=15,
#     )
#     linker.profile_columns(
#         [
#             "first_name",
#             ["surname"],
#             ["first_name", "surname"],
#             ["city", "first_name", "dob"],
#             ["first_name", "surname", "city", "dob"],
#         ],
#         top_n=15,
#         bottom_n=15,
#     )


@mark_with_dialects_including("duckdb")
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
    db_api = DuckDBAPI(connection=":memory:")

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    profile_columns(
        df,
        db_api,
        column_expressions,
        top_n=3,
        bottom_n=3,
    )


@mark_with_dialects_including("spark")
def test_profile_with_arrays_spark(spark, spark_api):
    spark_df = spark.read.parquet("tests/datasets/arrays_df.parquet")
    spark_df.persist()

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    profile_columns(
        spark_df,
        spark_api,
        column_expressions,
        top_n=3,
        bottom_n=3,
    )


@mark_with_dialects_including("sqlite")
def test_profile_using_sqlite():
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    con = sqlite3.connect(":memory:")

    df.to_sql("fake_data_1", con, if_exists="replace")

    db_api = SQLiteAPI(con)

    profile_columns(
        df,
        db_api,
        ["first_name", "surname", "first_name || surname"],
    )


# @pytest.mark.skip(reason="Uses Spark so slow and heavyweight")
@mark_with_dialects_including("spark")
def test_profile_using_spark(df_spark, spark_api):
    df_spark = df_spark.withColumn("blank", lit(None).cast(StringType()))

    profile_columns(
        df_spark,
        spark_api,
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    profile_columns(
        df_spark,
        spark_api,
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
            generate_raw_profile_dataset(df_spark, [["first_name", "blank"]], spark_api)
        )
        == 0
    )


@mark_with_dialects_including("duckdb")
def test_profile_null_columns(caplog):
    df = pd.DataFrame(
        [
            {"unique_id": 1, "test_1": 1, "test_2": None},
        ]
    )

    db_api = DuckDBAPI(connection=":memory:")

    profile_columns(df, db_api, ["test_1", "test_2"])
    captured_logs = caplog.text

    assert (
        "Warning: No charts produced for test_2 as the column only "
        "contains null values."
    ) in captured_logs

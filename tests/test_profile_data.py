import sqlite3

import pyarrow as pa
import pytest

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.internals.datasets import splink_dataset_labels
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.exceptions import SplinkException
from splink.internals.misc import ensure_is_list
from splink.internals.pipeline import CTEPipeline
from splink.internals.profile_data import (
    _col_or_expr_frequencies_raw_data_sql,
    profile_columns,
)
from splink.internals.sqlite.database_api import SQLiteAPI

from .decorator import mark_with_dialects_including


def generate_raw_profile_dataset(table, columns_to_profile, db_api):
    pipeline = CTEPipeline()

    column_expressions_raw = ensure_is_list(columns_to_profile)

    sql = _col_or_expr_frequencies_raw_data_sql(
        column_expressions_raw, table.physical_name
    )

    pipeline.enqueue_sql(sql, "__splink__df_all_column_value_frequencies")

    return db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_list()


@mark_with_dialects_including("duckdb")
def test_accuracy_analysis_after_profile_columns(fake_1000):
    # profile_columns() deletes Splink-created tables from the database; it must
    # also invalidate the cache, otherwise a later step that reuses a cached
    # term-frequency table queries a table that no longer exists (issue #3197).
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        ],
        blocking_rules_to_generate_predictions=[block_on("first_name")],
    )
    db_api = DuckDBAPI()
    sdf = db_api.register(fake_1000)
    linker = Linker(sdf, settings)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name")
    )

    # This deletes the tables Splink created, including the cached tf tables
    profile_columns(sdf, ["surname"])

    # Reusing the cached term-frequency tables must not reference a table that
    # profile_columns has deleted from the database.
    sdf_labels = db_api.register(splink_dataset_labels.fake_1000_labels)
    linker.evaluation.accuracy_analysis_from_labels_table(sdf_labels)


@mark_with_dialects_including("duckdb")
def test_profile_default_cols_duckdb(fake_1000):
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    profile_columns(df_sdf)


@mark_with_dialects_including("duckdb")
def test_profile_no_cols_raises(fake_1000):
    db_api = DuckDBAPI()
    df_sdf = db_api.register(fake_1000)

    with pytest.raises(SplinkException):
        profile_columns(df_sdf, column_expressions=["NULL"])


@mark_with_dialects_including("duckdb")
def test_profile_using_duckdb(fake_1000):
    db_api = DuckDBAPI(connection=":memory:")
    df_sdf = db_api.register(fake_1000.append_column("blank", pa.array(1000 * [None])))

    profile_columns(
        df_sdf,
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    profile_columns(
        df_sdf,
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
        len(generate_raw_profile_dataset(df_sdf, [["first_name", "blank"]], db_api))
        == 0
    )


@mark_with_dialects_including("duckdb")
def test_profile_saving(tmp_path, fake_1000):
    db_api = DuckDBAPI(connection=":memory:")
    df_sdf = db_api.register(fake_1000)

    profile_columns(
        df_sdf,
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    ch = profile_columns(
        df_sdf,
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
    ch.save(tmp_path / "my_temp_profile.html")
    ch.subcharts[0].save(tmp_path / "my_temp_profile_0.html")


@mark_with_dialects_including("duckdb")
def test_profile_with_arrays_duckdb():
    data = [
        {
            "id": 1,
            "forename": "Juan",
            "surname": "Pene",
            "offence_code_arr": None,
            "lat_long": {"lat": 22.730590, "lon": 9.388589},
        },
        {
            "id": 2,
            "forename": "Sarah",
            "surname": "Dowel",
            "offence_code_arr": [1, 2, 3],
            "lat_long": {"lat": 22.836322, "lon": 9.276112},
        },
        {
            "id": 3,
            "forename": "Leila",
            "surname": "Idin",
            "offence_code_arr": [1, 2, 3],
            "lat_long": {"lat": 37.770850, "lon": 95.689880},
        },
        {
            "id": 4,
            "forename": "Michaela",
            "surname": "Bose",
            "offence_code_arr": [1, 2, 3],
            "lat_long": None,
        },
    ]
    db_api = DuckDBAPI(connection=":memory:")
    df_sdf = db_api.register(data)

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    profile_columns(
        df_sdf,
        column_expressions,
        top_n=3,
        bottom_n=3,
    )


@mark_with_dialects_including("spark")
def test_profile_with_arrays_spark(spark, spark_api):
    spark_df = spark.read.parquet("tests/datasets/arrays_df.parquet")
    spark_df.persist()
    spark_df_sdf = spark_api.register(spark_df)

    column_expressions = ["forename", "surname", "offence_code_arr", "lat_long"]

    profile_columns(
        spark_df_sdf,
        column_expressions,
        top_n=3,
        bottom_n=3,
    )


@mark_with_dialects_including("sqlite")
def test_profile_using_sqlite(fake_1000):
    con = sqlite3.connect(":memory:")

    db_api = SQLiteAPI(con)
    df_sdf = db_api.register(fake_1000)

    profile_columns(
        df_sdf,
        ["first_name", "surname", "first_name || surname"],
    )


# @pytest.mark.skip(reason="Uses Spark so slow and heavyweight")
@mark_with_dialects_including("spark")
def test_profile_using_spark(fake_1000, spark_api):
    df_spark_sdf = spark_api.register(
        fake_1000.append_column("blank", pa.array(1000 * [None]))
    )

    profile_columns(
        df_spark_sdf,
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"],
        top_n=15,
        bottom_n=15,
    )
    profile_columns(
        df_spark_sdf,
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
            generate_raw_profile_dataset(
                df_spark_sdf, [["first_name", "blank"]], spark_api
            )
        )
        == 0
    )


@mark_with_dialects_including("duckdb")
def test_profile_null_columns(caplog):
    data = [
        {"unique_id": 1, "test_1": 1, "test_2": None},
    ]

    db_api = DuckDBAPI(connection=":memory:")
    df_sdf = db_api.register(data)

    profile_columns(df_sdf, ["test_1", "test_2"])
    captured_logs = caplog.text

    assert (
        "Warning: No charts produced for test_2 as the column only "
        "contains null values."
    ) in captured_logs

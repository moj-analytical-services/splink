import pytest
import sqlite3
import pandas as pd

from splink.blocking import block_using_rules

from splink.default_settings import complete_settings_dict

from pyspark.sql import Row


@pytest.fixture(scope="module")
def link_dedupe_data(spark):

    data_l = [
        {"unique_id": 1, "surname": "Linacre", "first_name": "Robin"},
        {"unique_id": 2, "surname": "Smith", "first_name": "John"},
    ]

    data_r = [
        {"unique_id": 7, "surname": "Linacre", "first_name": "Robin"},
        {"unique_id": 8, "surname": "Smith", "first_name": "John"},
        {"unique_id": 9, "surname": "Smith", "first_name": "Robin"},
    ]

    df_l = spark.createDataFrame(Row(**x) for x in data_l)
    df_r = spark.createDataFrame(Row(**x) for x in data_r)
    data_l.extend(data_r)
    df = spark.createDataFrame(Row(**x) for x in data_l)

    yield {"df": df, "df_l": df_l, "df_r": df_r}


def test_no_blocking(spark, link_dedupe_data):
    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": [],
    }
    settings = complete_settings_dict(settings, spark)
    df_l = link_dedupe_data["df_l"]
    df_r = link_dedupe_data["df_r"]
    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 1, 2, 2, 2]
    assert list(df["unique_id_r"]) == [7, 8, 9, 7, 8, 9]


def test_link_only(spark, link_dedupe_data):

    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": ["l.first_name = r.first_name", "l.surname = r.surname"],
    }
    settings = complete_settings_dict(settings, spark)
    df_l = link_dedupe_data["df_l"]
    df_r = link_dedupe_data["df_r"]
    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 2, 2]
    assert list(df["unique_id_r"]) == [7, 9, 8, 9]


def test_link_dedupe(spark, link_dedupe_data):

    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": ["l.first_name = r.first_name", "l.surname = r.surname"],
    }
    settings = complete_settings_dict(settings, spark=spark)
    df_l = link_dedupe_data["df_l"]
    df_r = link_dedupe_data["df_r"]
    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 2, 2, 7, 8]
    assert list(df["unique_id_r"]) == [7, 9, 8, 9, 9, 9]

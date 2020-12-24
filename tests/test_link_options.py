import pytest
from pyspark.sql import Row

from splink.blocking import block_using_rules
from splink.default_settings import complete_settings_dict


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


@pytest.fixture(scope="module")
def link_dedupe_data_repeat_ids(spark):

    data_l = [
        {"unique_id": 1, "surname": "Linacre", "first_name": "Robin"},
        {"unique_id": 2, "surname": "Smith", "first_name": "John"},
        {"unique_id": 3, "surname": "Smith", "first_name": "John"},
    ]

    data_r = [
        {"unique_id": 1, "surname": "Linacre", "first_name": "Robin"},
        {"unique_id": 2, "surname": "Smith", "first_name": "John"},
        {"unique_id": 3, "surname": "Smith", "first_name": "Robin"},
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


def test_link_only(spark, link_dedupe_data, link_dedupe_data_repeat_ids):

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

    df_l = link_dedupe_data_repeat_ids["df_l"]
    df_r = link_dedupe_data_repeat_ids["df_r"]
    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 2, 2, 3, 3]
    assert list(df["unique_id_r"]) == [1, 3, 2, 3, 2, 3]

    settings = {
        "link_type": "link_only",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": [],
    }
    settings = complete_settings_dict(settings, spark)

    df_comparison = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df_comparison.toPandas()
    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert list(df["unique_id_r"]) == [1, 2, 3, 1, 2, 3, 1, 2, 3]


def test_link_dedupe(spark, link_dedupe_data, link_dedupe_data_repeat_ids):

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

    df_l = link_dedupe_data_repeat_ids["df_l"]
    df_r = link_dedupe_data_repeat_ids["df_r"]

    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()
    df["u_l"] = df["unique_id_l"].astype(str) + df["_source_table_l"].str.slice(0, 1)
    df["u_r"] = df["unique_id_r"].astype(str) + df["_source_table_r"].str.slice(0, 1)

    df = df.sort_values(
        ["_source_table_l", "_source_table_r", "unique_id_l", "unique_id_r"]
    )

    assert list(df["u_l"]) == ["2l", "1l", "1l", "2l", "2l", "3l", "3l", "1r", "2r"]
    assert list(df["u_r"]) == ["3l", "1r", "3r", "2r", "3r", "2r", "3r", "3r", "3r"]

    settings = {
        "link_type": "link_and_dedupe",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": [],
    }
    settings = complete_settings_dict(settings, spark=spark)

    df_l = link_dedupe_data_repeat_ids["df_l"]
    df_r = link_dedupe_data_repeat_ids["df_r"]
    df = block_using_rules(settings, spark, df_l=df_l, df_r=df_r)
    df = df.toPandas()

    df["u_l"] = df["unique_id_l"].astype(str) + df["_source_table_l"].str.slice(0, 1)
    df["u_r"] = df["unique_id_r"].astype(str) + df["_source_table_r"].str.slice(0, 1)
    df = df.sort_values(
        ["_source_table_l", "unique_id_l", "_source_table_r", "unique_id_r"]
    )

    assert list(df["u_l"]) == [
        "1l",
        "1l",
        "1l",
        "1l",
        "1l",
        "2l",
        "2l",
        "2l",
        "2l",
        "3l",
        "3l",
        "3l",
        "1r",
        "1r",
        "2r",
    ]
    assert list(df["u_r"]) == [
        "2l",
        "3l",
        "1r",
        "2r",
        "3r",
        "3l",
        "1r",
        "2r",
        "3r",
        "1r",
        "2r",
        "3r",
        "2r",
        "3r",
        "3r",
    ]


def test_dedupe(spark, link_dedupe_data_repeat_ids):
    # This tests checks that we only get one result when a comparison is hit by multiple blocking rules
    settings = {
        "link_type": "dedupe_only",
        "comparison_columns": [{"col_name": "first_name"}, {"col_name": "surname"}],
        "blocking_rules": ["l.first_name = r.first_name", "l.surname = r.surname"],
    }
    settings = complete_settings_dict(settings, spark=None)
    df_l = link_dedupe_data_repeat_ids["df_l"]
    df = block_using_rules(settings, spark, df=df_l)
    df = df.toPandas()

    df = df.sort_values(["unique_id_l", "unique_id_r"])

    assert list(df["unique_id_l"]) == [2]
    assert list(df["unique_id_r"]) == [3]
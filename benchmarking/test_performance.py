import pytest

from splink.duckdb.duckdb_linker import DuckDBInMemoryLinker
from splink.spark.spark_linker import SparkLinker
from splink.sqlite.sqlite_linker import SQLiteLinker
import pandas as pd

import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


first_name_cc = {
    "column_name": "first_name",
    "comparison_levels": [
        {
            "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "first_name_l = first_name_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.7,
            "u_probability": 0.1,
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "levenshtein(first_name_l, first_name_r) <= 2",
            "m_probability": 0.2,
            "u_probability": 0.1,
            "label_for_charts": "Levenstein <= 2",
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.8,
        },
    ],
}

surname_cc = {
    "column_name": "surname",
    "comparison_levels": [
        {
            "sql_condition": "surname_l IS NULL OR surname_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "surname_l = surname_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

dob_cc = {
    "column_name": "dob",
    "comparison_levels": [
        {
            "sql_condition": "dob_l IS NULL OR dob_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "dob_l = dob_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

email_cc = {
    "column_name": "email",
    "comparison_levels": [
        {
            "sql_condition": "email_l IS NULL OR email_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "email_l = email_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}

city_cc = {
    "column_name": "city",
    "comparison_levels": [
        {
            "sql_condition": "city_l IS NULL OR city_r IS NULL",
            "label_for_charts": "Comparison includes null",
            "is_null_level": True,
        },
        {
            "sql_condition": "city_l = city_r",
            "label_for_charts": "Exact match",
            "m_probability": 0.9,
            "u_probability": 0.1,
            "tf_adjustment_column": "city",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "ELSE",
            "label_for_charts": "All other comparisons",
            "m_probability": 0.1,
            "u_probability": 0.9,
        },
    ],
}


settings_dict = {
    "proportion_of_matches": 0.01,
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.dob = r.dob",
        "l.city = r.city and l.first_name = r.first_name",
    ],
    "comparisons": [
        first_name_cc,
        surname_cc,
        dob_cc,
        email_cc,
        city_cc,
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "additional_columns_to_retain": ["group"],
    "max_iterations": 10,
}


def duckdb_performance(df, target_rows=1e6):

    linker = DuckDBInMemoryLinker(settings_dict, input_tables={"fake_data_1": df})

    linker.train_u_using_random_sampling(target_rows=target_rows)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    linker.predict()


def test_2_rounds_1k_duckdb(benchmark):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "target_rows": 1e6},
        rounds=2,
        iterations=1,
        warmup_rounds=0,
    )


def test_10_rounds_20k_duckdb(benchmark):
    df = pd.read_csv("./benchmarking/fake_20000_from_splink_demos.csv")
    benchmark.pedantic(
        duckdb_performance,
        kwargs={"df": df, "target_rows": 3e6},
        rounds=10,
        iterations=1,
        warmup_rounds=0,
    )


def spark_performance(df, target_rows):

    linker = SparkLinker(settings_dict, input_tables={"fake_data_1": df})

    linker.train_u_using_random_sampling(target_rows=target_rows)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    df = linker.predict().toPandas()


# conf.set("spark.default.parallelism", "8")
def test_3_rounds_20k_spark(benchmark):
    from pyspark.context import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()
    conf.set("spark.driver.memory", "12g")
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("spark.default.parallelism", "8")

    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    # df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df = spark.read.csv("./benchmarking/fake_20000_from_splink_demos.csv", header=True)

    benchmark.pedantic(
        spark_performance,
        kwargs={"df": df, "target_rows": 3e6},
        rounds=3,
        iterations=1,
        warmup_rounds=0,
    )


def sqlite_performance(con, target_rows=1e6):

    print("**** running sqlite benchmark ***")
    linker = SQLiteLinker(
        settings_dict,
        input_tables={"mydf": "input_df_tablename"},
        sqlite_connection=con,
    )

    linker.train_u_using_random_sampling(target_rows=1e6)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.train_m_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.train_m_using_expectation_maximisation(blocking_rule)
    df = linker.predict()
    df.as_record_dict()


def test_2_rounds_1k_sqlite(benchmark):
    import sqlite3

    con = sqlite3.connect(":memory:")
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df.to_sql("input_df_tablename", con)

    benchmark.pedantic(
        sqlite_performance,
        kwargs={"con": con, "target_rows": 1e6},
        rounds=2,
        iterations=1,
        warmup_rounds=0,
    )


def test_10_rounds_20k_sqlite(benchmark):
    import sqlite3

    con = sqlite3.connect(":memory:")
    df = pd.read_csv("./benchmarking/fake_20000_from_splink_demos.csv")
    df.to_sql("input_df_tablename", con)

    benchmark.pedantic(
        sqlite_performance,
        kwargs={"con": con, "target_rows": 3e6},
        rounds=10,
        iterations=1,
        warmup_rounds=0,
    )

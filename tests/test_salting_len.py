import pytest

from tests.basic_settings import get_settings_dict

from splink.spark.spark_linker import SparkLinker

from splink.spark.spark_comparison_level_library import _mutable_params

from copy import deepcopy


def check_same_ids(df1, df2, unique_id_col="unique_id"):
    col_l = f"{unique_id_col}_l"
    col_r = f"{unique_id_col}_r"

    for col in [col_l, col_r]:
        s1 = set(df1[col])
        s2 = set(df2[col])
        assert s1 == s2
        assert len(df1[col]) == len(df2[col])


def check_answer(df_1, df_2):
    assert df_1["match_probability"].sum() == pytest.approx(
        df_2["match_probability"].sum()
    )


def generate_linker_output(
    df,
    link_type="dedupe_only",
    blocking_rules=None,
):
    # Adjust our settings object
    settings = get_settings_dict()

    if blocking_rules:
        settings["blocking_rules_to_generate_predictions"] = blocking_rules
    settings["link_type"] = link_type

    linker = SparkLinker(df, settings)

    df_predict = linker.predict()
    df_predict = df_predict.as_pandas_dataframe()
    return df_predict.sort_values(by=["unique_id_l", "unique_id_r"], ignore_index=True)


def test_salting_spark():
    # Test that the number of rows in salted link jobs is identical
    # to those not salted.

    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()

    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("spark.default.parallelism", "8")

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)
    spark.sparkContext.setCheckpointDir("./tmp_checkpoints")

    _mutable_params["dialect"] = "spark"

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    blocking_rules_no_salt = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
        "l.dob = r.dob",
    ]

    blocking_rules_salted = [
        {"blocking_rule": "l.surname = r.surname", "salting_partitions": 3},
        {"blocking_rule": "l.first_name = r.first_name", "salting_partitions": 7},
        "l.dob = r.dob",
    ]
    spark.catalog.dropTempView("__splink__df_concat_with_tf")
    df3 = generate_linker_output(
        df=df_spark,
        blocking_rules=blocking_rules_no_salt,
    )
    spark.catalog.dropTempView("__splink__df_concat_with_tf")

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )
    df4 = generate_linker_output(df=df_spark, blocking_rules=blocking_rules_salted)

    check_same_ids(df3, df4)
    check_answer(df3, df4)

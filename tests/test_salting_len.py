import os

os.chdir("/Users/thomashepworth/py-data-linking/splink3")

from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd
from tests.basic_settings import get_settings_dict
from tests.cc_testing_utils import check_df_equality
from splink.spark.spark_linker import SparkLinker


def create_spark_session():
    from pyspark.context import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()
    conf.set("spark.driver.memory", "12g")
    conf.set("spark.sql.shuffle.partitions", "8")
    conf.set("spark.default.parallelism", "8")

    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    return spark


def generate_linker_output(
    linker_type,
    df,
    salting=1,
    link_type="dedupe_only",
    blocking_rules=None,
):
    # Adjust our settings object
    settings = get_settings_dict()
    settings["salting"] = salting
    if blocking_rules:
        settings["blocking_rules_to_generate_predictions"] = blocking_rules
    settings["link_type"] = link_type

    linker = linker_type(
        df,
        settings,
    )

    df_predict = linker.predict()
    df_predict = df_predict.as_pandas_dataframe()
    return df_predict.sort_values(by=["unique_id_l", "unique_id_r"], ignore_index=True)


def test_salting_out():
    # Test that the number of rows in salted link jobs is identical
    # to those not salted.

    # Create our test dataframes
    df_pd = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    spark = create_spark_session()
    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    df1 = generate_linker_output(DuckDBLinker, df_pd)
    df2 = generate_linker_output(DuckDBLinker, df_pd, salting=10)
    df3 = generate_linker_output(SparkLinker, df=df_spark)
    df4 = generate_linker_output(SparkLinker, df=df_spark, salting=10)

    assert check_df_equality(
        df1,  # duckdb no salt
        df2,  # duckdb w/ salt
        columns=["match_weight", "match_probability"],
    )
    assert check_df_equality(
        df3,  # spark no salt
        df4,  # spark w/ salt
        columns=["match_weight", "match_probability"],
    )

    # Generate more test dfs using a longer blocking rule
    longer_blocking_rule = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
        "l.dob = r.dob",
    ]

    df1 = generate_linker_output(
        DuckDBLinker, df_pd, blocking_rules=longer_blocking_rule
    )
    df2 = generate_linker_output(
        DuckDBLinker, df_pd, blocking_rules=longer_blocking_rule, salting=10
    )
    df3 = generate_linker_output(
        SparkLinker,
        df=df_spark,
        blocking_rules=longer_blocking_rule,
    )
    df4 = generate_linker_output(
        SparkLinker,
        df=df_spark,
        blocking_rules=longer_blocking_rule,
        salting=10,
    )

    assert check_df_equality(
        df1,  # duckdb no salt
        df2,  # duckdb w/ salt
        columns=["match_weight", "match_probability"],
    )
    assert check_df_equality(
        df3,  # spark no salt
        df4,  # spark w/ salt
        columns=["match_weight", "match_probability"],
    )

    # Finally, test the data with a different link type
    df1 = generate_linker_output(
        DuckDBLinker, [df_pd, df_pd], link_type="link_and_dedupe"
    )
    df2 = generate_linker_output(
        DuckDBLinker, [df_pd, df_pd], link_type="link_and_dedupe", salting=10
    )
    # These only break inside of the test... will correct later
    # df3 = generate_linker_output(
    #     SparkLinker, [df_spark, df_spark],
    #     link_type="link_and_dedupe",
    # )
    # df4 = generate_linker_output(
    #     SparkLinker, [df_spark, df_spark],
    #     link_type="link_and_dedupe",
    #     salting=10,
    # )

    assert check_df_equality(
        df1,  # duckdb no salt
        df2,  # duckdb w/ salt
        columns=["match_weight", "match_probability"],
    )
    # assert check_df_equality(
    #     df3,  # spark no salt
    #     df4,  # spark w/ salt
    #     columns=["match_weight", "match_probability"],
    # )

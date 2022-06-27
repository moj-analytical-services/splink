import pytest

from splink.duckdb.duckdb_linker import DuckDBLinker
import pandas as pd
from tests.basic_settings import get_settings_dict

from splink.spark.spark_linker import SparkLinker


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

    linker = linker_type(df, settings)

    df_predict = linker.predict()
    df_predict = df_predict.as_pandas_dataframe()
    return df_predict.sort_values(by=["unique_id_l", "unique_id_r"], ignore_index=True)


def test_salting_duckdb():
    # Test that the number of rows in salted link jobs is identical
    # to those not salted.

    # Create our test dataframes
    path = "./tests/datasets/fake_1000_from_splink_demos.csv"
    df_pd = pd.read_csv(path)
    df_pd_1 = pd.read_csv(path).sample(300)
    df_pd_2 = pd.read_csv(path).sample(300)
    df_pd_3 = pd.read_csv(path).sample(300)

    df1 = generate_linker_output(DuckDBLinker, df_pd)
    df2 = generate_linker_output(DuckDBLinker, df_pd, salting=10)

    check_same_ids(df1, df2)
    check_answer(df1, df2)

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

    check_same_ids(df1, df2)
    check_answer(df1, df2)

    df1 = generate_linker_output(
        DuckDBLinker, [df_pd_1, df_pd_2, df_pd_3], link_type="link_only"
    )
    df2 = generate_linker_output(
        DuckDBLinker, [df_pd_1, df_pd_2, df_pd_3], link_type="link_only", salting=10
    )

    check_same_ids(df1, df2)
    check_answer(df1, df2)

    df1 = generate_linker_output(
        DuckDBLinker, [df_pd_1, df_pd_2], link_type="link_and_dedupe"
    )
    df2 = generate_linker_output(
        DuckDBLinker, [df_pd_1, df_pd_2], link_type="link_and_dedupe", salting=10
    )

    check_same_ids(df1, df2)
    check_answer(df1, df2)


@pytest.mark.skip(reason="Slow")
def test_salting_spark(spark):
    # Test that the number of rows in salted link jobs is identical
    # to those not salted.

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    df3 = generate_linker_output(SparkLinker, df=df_spark)

    spark.catalog.dropTempView("__splink__df_concat_with_tf")

    df4 = generate_linker_output(SparkLinker, df=df_spark, salting=10)

    check_same_ids(df3, df4)
    check_answer(df3, df4)

    # Generate more test dfs using a longer blocking rule
    longer_blocking_rule = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
        "l.dob = r.dob",
    ]

    df3 = generate_linker_output(
        SparkLinker,
        df=df_spark,
        blocking_rules=longer_blocking_rule,
    )
    spark.catalog.dropTempView("__splink__df_concat_with_tf")

    df4 = generate_linker_output(
        SparkLinker,
        df=df_spark,
        blocking_rules=longer_blocking_rule,
        salting=10,
    )

    check_same_ids(df3, df4)
    check_answer(df3, df4)

import pytest

import splink.spark.blocking_rule_library as brl
from splink.spark.linker import SparkLinker
from tests.basic_settings import get_settings_dict


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


def test_salting_spark(spark):
    # Test that the number of rows in salted link jobs is identical
    # to those not salted.

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )

    blocking_rules_no_salt = [
        "l.surname = r.surname",
        "l.first_name = r.first_name",
        "l.dob = r.dob",
    ]

    blocking_rules_salted = [
        brl.exact_match_rule("surname", salting_partitions=3),
        {"blocking_rule": "l.first_name = r.first_name", "salting_partitions": 7},
        "l.dob = r.dob",
    ]
    spark.catalog.dropTempView("__splink__df_concat_with_tf")
    df1 = generate_linker_output(
        df=df_spark,
        blocking_rules=blocking_rules_no_salt,
    )
    spark.catalog.dropTempView("__splink__df_concat_with_tf")

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )
    df2 = generate_linker_output(df=df_spark, blocking_rules=blocking_rules_salted)

    check_same_ids(df1, df2)
    check_answer(df1, df2)

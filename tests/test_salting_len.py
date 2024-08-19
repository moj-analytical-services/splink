import pytest

from splink.internals.blocking_rule_library import block_on
from splink.internals.linker import Linker
from tests.basic_settings import get_settings_dict

from .decorator import mark_with_dialects_including


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
    spark_api,
    link_type="dedupe_only",
    blocking_rules=None,
):
    # Adjust our settings object
    settings = get_settings_dict()

    if blocking_rules:
        settings["blocking_rules_to_generate_predictions"] = blocking_rules
    settings["link_type"] = link_type

    linker = Linker(df, settings, spark_api)

    df_predict = linker.inference.predict()
    df_predict = df_predict.as_pandas_dataframe()
    return df_predict.sort_values(by=["unique_id_l", "unique_id_r"], ignore_index=True)


@mark_with_dialects_including("spark")
def test_salting_spark(spark, spark_api):
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
        block_on("surname", salting_partitions=3),
        {"blocking_rule": "l.first_name = r.first_name", "salting_partitions": 7},
        "l.dob = r.dob",
    ]
    spark.catalog.dropTempView("__splink__df_concat_with_tf")
    df1 = generate_linker_output(
        df=df_spark,
        spark_api=spark_api,
        blocking_rules=blocking_rules_no_salt,
    )
    spark.catalog.dropTempView("__splink__df_concat_with_tf")
    # TODO: this should perhaps be done when we instantiate a linker?
    spark_api._intermediate_table_cache.invalidate_cache()

    df_spark = spark.read.csv(
        "./tests/datasets/fake_1000_from_splink_demos.csv", header=True
    )
    df2 = generate_linker_output(
        df=df_spark, spark_api=spark_api, blocking_rules=blocking_rules_salted
    )

    check_same_ids(df1, df2)
    check_answer(df1, df2)

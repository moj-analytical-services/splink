import copy

from sparklink.blocking import cartestian_block, block_using_rules
from sparklink.gammas import add_gammas
from sparklink.iterate import iterate
from sparklink.expectation_step import run_expectation_step
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import logging

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def spark():

    try:
        import pyspark
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession

        conf = SparkConf()

        conf.set("spark.sql.shuffle.partitions", "1")
        conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
        sc = SparkContext.getOrCreate(conf=conf)

        spark = SparkSession(sc)
        SPARK_EXISTS = True
    except:
        SPARK_EXISTS = False

    if SPARK_EXISTS:
        print("Spark exists, running spark tests")
        yield spark
    else:
        spark = None
        log.error("Spark not available")
        print("Spark not available")
        yield spark


from pyspark.sql import SparkSession, Row


def test_expectation(spark, sqlite_con_1, params_1, gamma_settings_1):
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    df_comparison = block_using_rules(df, rules, spark=spark)

    df_gammas = add_gammas(
        df_comparison, gamma_settings_1, spark, include_orig_cols=False
    )

    df_e = iterate(df_gammas, spark, params_1, num_iterations=1)

    df_e_pd = df_e.toPandas()
    df_e_pd = df_e_pd.sort_values(["unique_id_l", "unique_id_r"])

    correct_list = [
        0.893617021,
        0.705882353,
        0.705882353,
        0.189189189,
        0.189189189,
        0.893617021,
        0.375,
        0.375,
    ]
    result_list = list(df_e_pd["match_probability"].astype(float))

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1])

    assert params_1.params["λ"] == pytest.approx(0.540922141)

    assert params_1.params["π"]["gamma_0"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.087438272, abs=0.0001)
    assert params_1.params["π"]["gamma_1"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.160167628, abs=0.0001)


def test_iterate(spark, sqlite_con_1, params_1, gamma_settings_1):

    original_params = copy.deepcopy(params_1)
    dfpd = pd.read_sql("select * from test1", sqlite_con_1)
    df = spark.createDataFrame(dfpd)

    rules = [
        "l.mob = r.mob",
        "l.surname = r.surname",
    ]

    df_comparison = block_using_rules(df, rules, spark=spark)

    df_gammas = add_gammas(
        df_comparison, gamma_settings_1, spark, include_orig_cols=False
    )

    df_e = iterate(df_gammas, spark, params_1, num_iterations=2)

    df_e_pd = df_e.toPandas()
    df_e_pd = df_e_pd.sort_values(["unique_id_l", "unique_id_r"])


    correct_list = [
        0.658602114,
        0.796821727,
        0.796821727,
        0.189486495,
        0.189486495,
        0.658602114,
        0.495063367,
        0.495063367,
    ]
    result_list = list(df_e_pd["match_probability"].astype(float))

    for i in zip(result_list, correct_list):
        assert i[0] == pytest.approx(i[1], abs=0.0001)

    assert params_1.params["λ"] == pytest.approx(0.534993426, abs=0.0001)

    assert params_1.params["π"]["gamma_0"]["prob_dist_match"]["level_0"][
        "probability"
    ] == pytest.approx(0.088546179, abs=0.0001)
    assert params_1.params["π"]["gamma_1"]["prob_dist_non_match"]["level_1"][
        "probability"
    ] == pytest.approx(0.109234086, abs=0.0001)

    ## Test whether the params object is correctly storing the iteration history

    assert params_1.param_history[0] == original_params

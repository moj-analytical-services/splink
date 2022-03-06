import pytest


import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def spark():

    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession

    conf = SparkConf()

    conf.set("spark.driver.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "12")

    sc = SparkContext.getOrCreate(conf=conf)

    spark = SparkSession(sc)

    yield spark


@pytest.fixture(scope="module")
def df_spark(spark):
    df = spark.read.csv("./tests/datasets/fake_1000_from_splink_demos.csv", header=True)
    df.persist()
    yield df

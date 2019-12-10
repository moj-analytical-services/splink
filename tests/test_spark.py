from sparklink.blocking import cartestian_block
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
import logging

log = logging.getLogger(__name__)


@pytest.fixture(scope='module')
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

def test_cartesian(spark, caplog):

    if spark:
        original_data = [
            {"unique_id": 1, "name": "Robin"},
            {"unique_id": 2, "name": "John"},
            {"unique_id": 3, "name": "James"}
        ]

        correct_answer = [
            {'name_l': 'Robin', 'name_r': 'John', 'unique_id_l': 1, 'unique_id_r': 2},
            {'name_l': 'Robin', 'name_r': 'James', 'unique_id_l': 1, 'unique_id_r': 3},
            {'name_l': 'John', 'name_r': 'James', 'unique_id_l': 2, 'unique_id_r': 3}
        ]

        df = spark.createDataFrame(Row(**x) for x in original_data)


        df_c = cartestian_block(df, df.columns, spark=spark)

        df_correct = pd.DataFrame(correct_answer)
        sort_order = list(df_correct.columns)
        df_correct = df_correct.sort_values(sort_order)
        df_test = df_c.toPandas().sort_values(sort_order)

        assert_frame_equal(df_correct, df_test)


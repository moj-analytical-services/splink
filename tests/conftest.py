import pytest


import logging

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def spark():

    try:
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql import types

        conf = SparkConf()

        conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
        conf.set("spark.driver.extraClassPath", "jars/scala-udf-similarity-0.0.9.jar")
        conf.set("spark.jars", "jars/scala-udf-similarity-0.0.9.jar")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.sql.shuffle.partitions", "12")

        sc = SparkContext.getOrCreate(conf=conf)

        spark = SparkSession(sc)

        udfs = [
            ("jaro_winkler_sim", "JaroWinklerSimilarity", types.DoubleType()),
            ("jaccard_sim", "JaccardSimilarity", types.DoubleType()),
            ("cosine_distance", "CosineDistance", types.DoubleType()),
            ("Dmetaphone", "DoubleMetaphone", types.StringType()),
            ("QgramTokeniser", "QgramTokeniser", types.StringType()),
            ("Q3gramTokeniser", "Q3gramTokeniser", types.StringType()),
            ("Q4gramTokeniser", "Q4gramTokeniser", types.StringType()),
            ("Q5gramTokeniser", "Q5gramTokeniser", types.StringType()),
            ("DmetaphoneAlt", "DoubleMetaphoneAlt", types.StringType()),
        ]

        for a, b, c in udfs:
            spark.udf.registerJavaFunction(a, "uk.gov.moj.dash.linkage." + b, c)

        rt = types.ArrayType(
            types.StructType(
                [
                    types.StructField("_1", types.StringType()),
                    types.StructField("_2", types.StringType()),
                ]
            )
        )

        spark.udf.registerJavaFunction(
            name="DualArrayExplode",
            javaClassName="uk.gov.moj.dash.linkage.DualArrayExplode",
            returnType=rt,
        )
        SPARK_EXISTS = True

    except:
        SPARK_EXISTS = False

    if SPARK_EXISTS:
        print("Spark exists, running spark tests")
        yield spark
    else:
        spark = None
        logger.error("Spark not available")
        print("Spark not available")
        yield spark

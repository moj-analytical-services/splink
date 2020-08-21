import pytest
from splink import Splink
import pandas as pd
import pyspark.sql.functions as f
import pyspark
import warnings


@pytest.fixture(scope="module")
def spark():

    try:

        import pyspark
        from pyspark import SparkContext, SparkConf
        from pyspark.sql import SparkSession
        from pyspark.sql import types

        conf = SparkConf()

        conf.set("spark.sql.shuffle.partitions", "1")
        conf.set("spark.jars.ivy", "/home/jovyan/.ivy2/")
        conf.set("spark.driver.extraClassPath", "jars/scala-udf-similarity-0.0.6.jar")
        conf.set("spark.jars", "jars/scala-udf-similarity-0.0.6.jar")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.sql.shuffle.partitions", "24")

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
        ]

        for a, b, c in udfs:
            spark.udf.registerJavaFunction(a, "uk.gov.moj.dash.linkage." + b, c)
        
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
        


@pytest.fixture(scope="module")
def sparkdf(spark):       
        
    data = [
         {"surname": "smith", "firstname": "john"},
         {"surname": "smith", "firstname": "john"}, 
         {"surname": "smithe","firstname": "john"}


    ]
    
    dfpd = pd.DataFrame(data)
    df = spark.createDataFrame(dfpd)
    yield df
    
    
def test_freq_adj_divzero(spark, sparkdf):
    
    # create settings object that requests term_freq_adjustments on column 'weird'
    
    settings = {
    "link_type": "dedupe_only",
    "blocking_rules": [
    "l.surname = r.surname",

    ],
    "comparison_columns": [
        {
            "col_name": "firstname",
            "num_levels": 3,
        },
        {
            "col_name": "surname",
            "num_levels": 3,
            "term_frequency_adjustments": True
        },
        {
            "col_name": "weird",
            "num_levels": 3,
            "term_frequency_adjustments": True
        }
 
    ],
    "additional_columns_to_retain": ["unique_id"],
    "em_convergence": 0.01
    }
    
    
    sparkdf = sparkdf.withColumn("unique_id", f.monotonically_increasing_id())
    # create column weird in a way that could trigger a div by zero on the average adj calculation before the fix
    sparkdf = sparkdf.withColumn("weird",f.lit(None))
    
    
    try:
        linker = Splink(settings, spark, df=sparkdf)
        notpassing = False
    except ZeroDivisionError: 
        notpassing = True
        
    assert ( notpassing == False )
    
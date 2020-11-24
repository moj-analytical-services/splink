
import pyspark.sql.functions as f
import pandas as pd

from splink.diagnostics import _splink_score_hist
import pytest





    
def test_score_hist_adjusted(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function is given a column name to take splink score from
    test that a column with values full of 1.0 s has all percentiles as 1.0
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("df_dummy", f.lit(1.0))
    
  

    res = _splink_score_hist(df, spark=spark,adjusted="df_dummy")

    assert isinstance(res, dict)
    
    assert all(value == 1.0 for value in res.values())
    
    
    
def test_score_hist_tf(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function uses the default col to take splink score from
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0-(f.rand()/10) )
    
    
  

    res = _splink_score_hist(df, spark=spark)
    
    assert isinstance(res, dict)
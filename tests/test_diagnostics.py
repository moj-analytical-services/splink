import pyspark.sql.functions as f
import pandas as pd

from splink.diagnostics import _splink_score_histogram
import pytest


def test_score_hist_adjusted(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function is given a column name to take splink score from
    
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("df_dummy", f.lit(1.0))

    res = _splink_score_histogram(df, spark=spark, adjusted="df_dummy")

    assert all(value != None for value in res.count_rows.values)
    assert isinstance(res, pd.DataFrame)
    assert res.count_rows[7] != 0.0


def test_score_hist_tf(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function uses the default col to take splink score from
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))

    res = _splink_score_histogram(df, spark=spark)

    assert isinstance(res, pd.DataFrame)

    
    
    
def test_score_hist_splits(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed with non-default splits
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))
    
    mysplits = [0.3,0.6]

    res = _splink_score_histogram(df, spark=spark,splits=mysplits)

    assert isinstance(res, pd.DataFrame)
    assert res.count_rows.count() == 3
    assert res.count_rows.sum() == res.count_rows.cumsum()[2]

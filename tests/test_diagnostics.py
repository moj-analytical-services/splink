import pyspark.sql.functions as f
import pandas as pd
import json

from splink.diagnostics import (
    splink_score_histogram,
    _calc_probability_density,
    _create_probability_density_plot,
)
import pytest


def test_score_hist_score(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function is given a column name to take splink score from
    
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("df_dummy", f.lit(1.0))

    res = _calc_probability_density(df, spark=spark, score_colname="df_dummy")

    assert all(value != None for value in res.count_rows.values)
    assert isinstance(res, pd.DataFrame)


def test_score_hist_tf(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed when function uses the default col to take splink score from
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))

    res = _calc_probability_density(df, spark=spark)

    assert isinstance(res, pd.DataFrame)


def test_score_hist_splits(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that a dataframe gets processed with non-default splits list
    test binwidths and normalised probability densities sum up to 1.0
    
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))

    mysplits = [0.3, 0.6]

    res = _calc_probability_density(df, spark=spark, buckets=mysplits)

    assert isinstance(res, pd.DataFrame)
    assert res.count_rows.count() == 3
    assert res.count_rows.sum() == res.count_rows.cumsum()[2]
    assert res.binwidth.sum() == 1.0
    assert res.normalised.sum() == 1.0


def test_score_hist_intsplits(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
  
    test integer value in splits variable
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))

    res2 = _calc_probability_density(df, spark=spark, buckets=5)

    assert res2.count_rows.count() == 5
    assert res2.binwidth.sum() == 1.0
    assert res2.normalised.sum() == 1.0


def test_score_hist_output_json(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
  
    test chart exported as dictionary is in fact a valid dictionary
    """

    altair_installed = True
    try:
        import altair as alt
    except ImportError:
        altair_installed = False

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df = spark.createDataFrame(dfpd)
    df = df.withColumn("tf_adjusted_match_prob", 1.0 - (f.rand() / 10))

    res3 = _calc_probability_density(df, spark=spark, buckets=5)

    if altair_installed:
        assert isinstance(_create_probability_density_plot(res3).to_dict(), dict)
    else:
        assert isinstance(_create_probability_density_plot(res3), dict)


def test_prob_density(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
  
     a test that checks that probability density is computed correctly. 
     explicitly define a dataframe with tf_adjusted_match_prob = [0.1, 0.3, 0.5, 0.7, 0.9] 
     and make sure that the probability density is the correct value (0.2) with all 5 bins
     
    """

    dfpd = pd.DataFrame([0.1, 0.3, 0.5, 0.7, 0.9], columns=["tf_adjusted_match_prob"])
    spdf = spark.createDataFrame(dfpd)

    res = _calc_probability_density(spdf, spark=spark, buckets=5)
    assert all(value == pytest.approx(0.2) for value in res.normalised.values)

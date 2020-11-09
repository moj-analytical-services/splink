import pyspark.sql.functions as f
import pandas as pd
from splink.gammas import add_gammas
from splink.diagnostics import vif_gammas
import pytest

# For further info about this test see https://github.com/moj-analytical-services/splink/issues/132
def test_vif_gammas(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    use fixture data that is independent to ensure vif_gammas works with typical data
    (in this case with low association / correlation between columns)

    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df_gammas = spark.createDataFrame(dfpd)
    df_gammas = df_gammas.filter("true_match = 0")

    res = vif_gammas(df_gammas, spark=spark, sampleratio=1.0).toPandas()

    for val in res.vif.values:
        assert pytest.approx(val) == 1.0


def test_vif_no_gammas(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that when input doesnt have gamma columns function exits gracefully

    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df_gammas = spark.createDataFrame(dfpd)
    df_gammas = df_gammas.withColumn("-", f.lit("1.0"))
    df_gammas = df_gammas.select("-")

    res = vif_gammas(df_gammas, spark=spark, sampleratio=0.05).toPandas()

    assert (res[""].values).size == 0


def test_vif_fully_correlated(spark, gamma_settings_4, params_4, sqlite_con_4):

    """
    test that when input data has some columns that are fully correlated / associated function deals with it gracefully
    """

    dfpd = pd.read_sql("select * from df", sqlite_con_4)
    df_gammas = spark.createDataFrame(dfpd)
    df_gammas = df_gammas.withColumn("gamma_dummy1", f.lit("1.0"))
    df_gammas = df_gammas.withColumn("gamma_dummy2", f.lit("1.0"))

    res = vif_gammas(df_gammas, spark=spark, sampleratio=0.05).toPandas()

    assert res.vif.isnull().values.any() == True

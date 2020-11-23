from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext

from typing import Callable

from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from .check_types import check_types
import warnings


@check_types
def vif_gammas(df_gammas: DataFrame, spark: SparkSession, sampleratio: float = 1.0):

    """splink diagnostic of multicollinearity in gamma values
    
    We want to check if  the gammas  of the input variables of the models we are using suffer from multicollinearity. 
    Since the columns information is transformed into  gammas , and it is the information from the gammas 
    which the model 'sees', therefore it is corelation on the gammas which is potentially problematic.
    
    
    
        Args:
        
            df_gammas (DataFrame): A dataframe of record comparisons containing gamma comaprisno values, 
            e.g. as produced by the splink.gammas.add_gammas function
            
            spark (SparkSession): SparkSession object
            
            sampleratio (float,optional) : Fraction of rows to sample, range [0.0, 1.0]. 
            If 1.0 no sampling takes place
            
            
        
        Returns:
            
            (DataFrame) : Spark dataframe of the gamma variables Variance Inflation Factors (VIFs)
            
        
        """

    collist = []
    viflist = []

    dfvariables = df_gammas.columns

    # get gamma_ columms only

    gammaonly = [s for s in dfvariables if str(s).startswith("gamma")]

    # if no gamma columns available exit function gracefully
    if gammaonly == []:
        warnings.warn("no gamma (agreement vector) columns available")

        emptyschema = StructType([StructField("", StringType(), True)])
        return spark.createDataFrame([], emptyschema)

    # only keep gamma_ columns
    df_gammas = df_gammas.select(gammaonly).sample(
        withReplacement=False, fraction=sampleratio, seed=42
    )

    # cast gamma_ columns to double in case they are not
    df_gammas = df_gammas.select(*(f.col(c).cast("double") for c in df_gammas.columns))

    # clamp values to either 0 or 1 or NULL in case of -1

    for gammacol in gammaonly:
        df_gammas = df_gammas.withColumn(
            gammacol,
            f.when(df_gammas[gammacol] < 0.0, None)
            .when(df_gammas[gammacol] > 0.0, 1.0)
            .otherwise(0.0),
        )

    # drop any NULLs
    df_gammas = df_gammas.na.drop()
    # add a dummy unused column in the start of the dataframe to make the round robin thing work on the vif calcs.
    df_gammas = df_gammas.withColumn("_", f.lit("_")).select("_", *gammaonly)
    vifcols = df_gammas.columns

    # VIF computation

    for i in range(1, len(vifcols)):

        # round robin computation of r_squared and vif from the available vars
        train_t = df_gammas.rdd.map(
            lambda x: [Vectors.dense(x[1:i] + x[i + 1 :]), x[i]]
        ).toDF(["features", "label"])

        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_t)
        predictions = lr_model.transform(train_t)
        evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label")
        r_sq = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

        if r_sq != 1.0:
            vif = 1.0 / (1.0 - r_sq)
        else:
            cc = vifcols[i]
            warnings.warn(
                f"variable {cc} is totally correlated/associated with another variable"
            )
            vif = None

        collist.append(vifcols[i])
        viflist.append(vif)

    vifSchema = StructType(
        [StructField("col", StringType()), StructField("vif", DoubleType())]
    )

    return spark.createDataFrame(zip(collist, viflist), vifSchema)


@check_types
def _splink_score_hist_df(
    df_e: DataFrame,
    spark: SparkSession,
    percentiles=[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95],
    adjusted=None,
    error: float = 0.1,
):
    """splink score histogram diagnostic 
    
       Uses approxQuantile function.This method implements a variation of the Greenwald-Khanna algorithm (with some speed optimizations).
       It allows faster computation of the quantile at probability p up to error err so the result for N elements is
       
       floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
    
    
    
    
        Args:
        
            df_e (DataFrame): A dataframe of record comparisons containing a splink score, 
            e.g. as produced by the .... function
            
            spark (SparkSession): SparkSession object
            
            adjusted (bool) : is the score tf adjusted or not? defaults to True
            
            percentiles :
            
            error (float,optional) : allowed error. Defaults to 0.1. Lower values for more exact computations but more computationally expensive ones
            
            
            
            
        
        Returns:
            
            (DataFrame) : Spark dataframe of quantiles for appropriate splink score variable ready to be plotted.
            
        
    """
    if adjusted == None:
        return dict(
            zip(
                percentiles,
                df_e.approxQuantile("tf_adjusted_match_prob", percentiles, error),
            )
        )
    else:
        return dict(zip(percentiles, df_e.approxQuantile(adjusted, percentiles, error)))

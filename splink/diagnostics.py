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
def _splink_score_hist(
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
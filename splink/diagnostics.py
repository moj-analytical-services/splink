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


import pandas as pd
import warnings


altair_installed = True
try:
    import altair as alt
except ImportError:
    altair_installed = False


@check_types
def _splink_score_histogram(
    df_e: DataFrame,
    spark: SparkSession,
    splits=[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95],
    adjusted=None,
):

    """splink score histogram diagnostic 
    
    
        Args:
    
        
            df_e (DataFrame): A dataframe of record comparisons containing a splink score, 
            e.g. as produced by the .... function
            
            spark (SparkSession): SparkSession object
            
            adjusted : is the score tf adjusted or not? defaults to None
            
            splits : list of splits for histogram bins. Has [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95] for default
            
        
        Returns:
            
            (DataFrame) : pandas dataframe of histogram bins for appropriate splink score variable ready to be plotted.
            
        
    """

    # ensure all values are included in histogram

    if splits[0] != 0:
        splits = [0.0] + splits

    if splits[-1] != 1.0:
        splits = splits + [1.0]

    # If adjusted is left empty as the default then tf_adjusted_match_prob is used. Otherwise the variable in adjusted is used

    if adjusted == None:
        hist = (
            df_e.select("tf_adjusted_match_prob")
            .rdd.flatMap(lambda x: x)
            .histogram(splits)
        )
    else:
        hist = df_e.select(adjusted).rdd.flatMap(lambda x: x).histogram(splits)

    hist[1].append(None)
    hist_df = pd.DataFrame({"splink_score_bin_low": hist[0], "count_rows": hist[1]})
    hist_df["splink_score_bin_high"] = hist_df["splink_score_bin_low"].shift(-1)
    hist_df = hist_df.drop(hist_df.tail(1).index)

    return hist_df


def create_diagnostic_plot(hist_df):
    """splink score histogram diagnostic 
    
    
        Args:
    
        
            hist_df (pandas DataFrame): A dataframe of record comparisons containing a splink score, 
            e.g. as produced by the _splink_score_histogram function

            
        
        Returns:
            
            Nothing. But plots an altair chart
            
        
    """

    h_alt = (
        alt.Chart(hist_df, title="splink score histogram")
        .mark_bar()
        .encode(
            x=alt.X(
                "splink_score_bin_low:Q",
                bin="binned",
                axis=alt.Axis(title="splink score"),
            ),
            x2="splink_score_bin_high:Q",
            y="count_rows:Q",
            tooltip=[alt.Tooltip("count_rows:Q", title="count")],
        )
        .properties(width=700, height=200)
        .configure_title(fontSize=14)
    )

    return h_alt

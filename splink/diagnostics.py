from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext
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


hist_def_dict = {
    "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
    "config": {
        "title": {"fontSize": 14},
        "view": {"continuousHeight": 300, "continuousWidth": 400},
    },
    "data": {"values": None},
    "encoding": {
        "tooltip": [{"field": "count_rows", "title": "count", "type": "quantitative"}],
        "x": {
            "axis": {"title": "splink score"},
            "bin": "binned",
            "field": "splink_score_bin_low",
            "type": "quantitative",
        },
        "x2": {"field": "splink_score_bin_high"},
        "y": {
            "field": "normalised",
            "type": "quantitative",
            "axis": {"title": "Probability density"},
        },
        "height": 200,
        "mark": "bar",
        "title": "Estimated Probability Density",
        "width": 700,
    },
}


@check_types
def _splink_score_histogram(
    df_e: DataFrame, spark: SparkSession, splits=None, score_colname=None,
):

    """splink score histogram diagnostic 
    
    
        Args:
    
        
            df_e (DataFrame): A dataframe of record comparisons containing a splink score, 
            e.g. as produced by the .... function
            
            spark (SparkSession): SparkSession object
            
            score_colname : is the score in another column? defaults to None
            
            splits : list of splits for histogram bins. Has [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95] for default
            
        
        Returns:
            
            (DataFrame) : pandas dataframe of histogram bins for appropriate splink score variable ready to be plotted.
            
        
    """

    # if splits a list then use it. if None... then create default. if integer then create equal bins

    if isinstance(splits, int) and splits != 0:
        splits = [(x / splits) for x in list(range(splits))]
    elif splits == None:
        splits = [(x / 100) for x in list(range(100))]

    # ensure 0.0 and 1.0 are included in histogram

    if splits[0] != 0:
        splits = [0.0] + splits

    if splits[-1] != 1.0:
        splits = splits + [1.0]

    # If score_colname is used then use that. if score_colname not used if tf_adjusted_match_prob exists it is used.
    # Otherwise match_probability is used or if that doesnt exit a warning is fired and function exits

    if score_colname:
        hist = df_e.select(score_colname).rdd.flatMap(lambda x: x).histogram(splits)

    elif "tf_adjusted_match_prob" in df_e.columns:

        hist = (
            df_e.select("tf_adjusted_match_prob")
            .rdd.flatMap(lambda x: x)
            .histogram(splits)
        )

    elif "match_probability" in df_e.columns:

        hist = (
            df_e.select("match_probability").rdd.flatMap(lambda x: x).histogram(splits)
        )

    else:
        warnings.warn("Cannot find score column")

    hist[1].append(None)
    hist_df = pd.DataFrame({"splink_score_bin_low": hist[0], "count_rows": hist[1]})
    hist_df["splink_score_bin_high"] = hist_df["splink_score_bin_low"].shift(-1)
    hist_df = hist_df.drop(hist_df.tail(1).index)

    # take into account the bin width
    hist_df["binwidth"] = (
        hist_df["splink_score_bin_high"] - hist_df["splink_score_bin_low"]
    )
    hist_df["freqdensity"] = hist_df["count_rows"] / hist_df["binwidth"]

    sumfreqdens = hist_df.freqdensity.sum()
    hist_df["normalised"] = hist_df["freqdensity"] / sumfreqdens

    return hist_df


def create_diagnostic_plot(hist_df):
    """splink score histogram diagnostic 
    
    
        Args:
    
        
            hist_df (pandas DataFrame): A dataframe of record comparisons containing a splink score, 
            as produced by the _splink_score_histogram function

            
        
        Returns:
            
            if altair is installed a plot. if not the plot dictionary so it can be plotted in a different way
          
    """

    data = hist_df.to_dict(orient="records")
    hist_def_dict["data"]["values"] = data

    if altair_installed:
        return alt.Chart.from_dict(hist_def_dict)
    else:
        return hist_def_dict

#import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_library as cld
import splink.duckdb.duckdb_comparison_template_library as ctld
import splink.spark.spark_comparison_library as cls
import splink.spark.spark_comparison_template_library as ctls

from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


def test_duckdb_date_comparison_run():

    ctld.date_comparison("date")


def test_spark_date_comparison_run():

    ctls.date_comparison("date")

def test_date_comparison_error_logger():

    # Differing lengths between thresholds and units
    with pytest.raises(ValueError):
        ctld.date_comparison("date",
                             datediff_thresholds=[[1,2], ['month']]
        )
    # Check metric and threshold are the correct way around
    with pytest.raises(ValueError):
        ctld.date_comparison("date",
                             datediff_thresholds=[['month'], [1,2]]
        )
    # Invalid metric
    with pytest.raises(ValueError):
        ctld.date_comparison("date", 
                            datediff_thresholds=[[1], ["dy"]]
        )
    # Threshold len == 0
    with pytest.raises(ValueError):
        ctld.date_comparison("date", 
                            datediff_thresholds=[[], ["day"]]
        )
    # Metric len == 0
    with pytest.raises(ValueError):
        ctld.date_comparison("date", 
                            datediff_thresholds=[[1], []]
        )


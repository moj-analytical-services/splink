import pandas as pd

import splink.duckdb.duckdb_comparison_library as cld
import splink.duckdb.duckdb_comparison_template_library as ctld
import splink.spark.spark_comparison_library as cls
import splink.spark.spark_comparison_template_library as ctls

from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker


ctl.date_comparison("date")
ctls.date_comparison("date")


def test_duckdb_date_comparison_run():

    ctld.date_comparison()


def test_spark_date_comparison_run():

    ctls.date_comparison()

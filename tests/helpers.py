import sqlite3
from abc import ABC, abstractmethod

import pandas as pd

import splink.duckdb.duckdb_comparison_level_library as cll_duckdb
import splink.duckdb.duckdb_comparison_library as cl_duckdb
import splink.spark.spark_comparison_level_library as cll_spark
import splink.spark.spark_comparison_library as cl_spark
import splink.sqlite.sqlite_comparison_level_library as cll_sqlite
import splink.sqlite.sqlite_comparison_library as cl_sqlite
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.spark.spark_linker import SparkLinker
from splink.sqlite.sqlite_linker import SQLiteLinker


class TestHelper(ABC):
    @property
    @abstractmethod
    def linker(self):
        pass

    def extra_linker_args(self, num_frames=1):
        return {}

    @abstractmethod
    def convert_frame(self, df):
        pass

    def load_frame_from_csv(self, path):
        return pd.read_csv(path)

    @property
    @abstractmethod
    def cll(self):
        pass

    @property
    @abstractmethod
    def cl(self):
        pass


class DuckDBTestHelper(TestHelper):
    @property
    def linker(self):
        return DuckDBLinker

    def convert_frame(self, df):
        return df

    @property
    def cll(self):
        return cll_duckdb

    @property
    def cl(self):
        return cl_duckdb


class SparkTestHelper(TestHelper):
    def __init__(self, spark):
        self.spark = spark

    @property
    def linker(self):
        return SparkLinker

    def extra_linker_args(self, num_frames=1):
        return {"spark": self.spark}

    def convert_frame(self, df):
        spark_frame = self.spark.createDataFrame(df)
        spark_frame.persist()
        return spark_frame

    def load_frame_from_csv(self, path):
        df = self.spark.read.csv(path, header=True)
        df.persist()
        return df

    @property
    def cll(self):
        return cll_spark

    @property
    def cl(self):
        return cl_spark


class SQLiteTestHelper(TestHelper):
    def __init__(self):
        from rapidfuzz.distance.Levenshtein import distance

        def lev_wrap(str_l, str_r):
            return distance(str(str_l), str(str_r))

        con = sqlite3.connect(":memory:")
        con.create_function("levenshtein", 2, lev_wrap)
        self.con = con
        self._frame_counter = 0

    @property
    def linker(self):
        return SQLiteLinker

    def extra_linker_args(self, num_frames=1):
        return {
            "connection": self.con,
            "input_table_aliases": self._get_input_name(num_frames)
        }

    def _get_input_name(self, num_frames):
        names = []
        for _i in range(num_frames):
            names.append(f"input_alias_{self._frame_counter}")
            self._frame_counter += 1
        return names

    def convert_frame(self, df):
        return df

    @property
    def cll(self):
        return cll_sqlite

    @property
    def cl(self):
        return cl_sqlite


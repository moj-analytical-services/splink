import sqlite3
from abc import ABC, abstractmethod

import pandas as pd

import splink.duckdb.duckdb_comparison_level_library as cll_duckdb
import splink.duckdb.duckdb_comparison_library as cl_duckdb
import splink.duckdb.duckdb_comparison_template_library as ctl_duckdb
import splink.postgres.postgres_comparison_level_library as cll_postgres
import splink.postgres.postgres_comparison_library as cl_postgres
import splink.postgres.postgres_comparison_template_library as ctl_postgres
import splink.spark.spark_comparison_level_library as cll_spark
import splink.spark.spark_comparison_library as cl_spark
import splink.spark.spark_comparison_template_library as ctl_spark
import splink.sqlite.sqlite_comparison_level_library as cll_sqlite
import splink.sqlite.sqlite_comparison_library as cl_sqlite
import splink.sqlite.sqlite_comparison_template_library as ctl_sqlite
from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.postgres.postgres_linker import PostgresLinker
from splink.spark.spark_linker import SparkLinker
from splink.sqlite.sqlite_linker import SQLiteLinker


class TestHelper(ABC):
    @property
    @abstractmethod
    def Linker(self):
        pass

    def extra_linker_args(self):
        return {}

    @abstractmethod
    def convert_frame(self, df):
        pass

    def load_frame_from_csv(self, path):
        return pd.read_csv(path)

    def load_frame_from_parquet(self, path):
        return pd.read_parquet(path)

    @property
    @abstractmethod
    def cll(self):
        pass

    @property
    @abstractmethod
    def cl(self):
        pass

    @property
    @abstractmethod
    def ctl(self):
        pass


class DuckDBTestHelper(TestHelper):
    @property
    def Linker(self):
        return DuckDBLinker

    def convert_frame(self, df):
        return df

    @property
    def cll(self):
        return cll_duckdb

    @property
    def cl(self):
        return cl_duckdb

    @property
    def ctl(self):
        return ctl_duckdb


class SparkTestHelper(TestHelper):
    def __init__(self, spark):
        self.spark = spark

    @property
    def Linker(self):
        return SparkLinker

    def extra_linker_args(self):
        return {"spark": self.spark}

    def convert_frame(self, df):
        spark_frame = self.spark.createDataFrame(df)
        spark_frame.persist()
        return spark_frame

    def load_frame_from_csv(self, path):
        df = self.spark.read.csv(path, header=True)
        df.persist()
        return df

    def load_frame_from_parquet(self, path):
        df = self.spark.read.parquet(path)
        df.persist()
        return df

    @property
    def cll(self):
        return cll_spark

    @property
    def cl(self):
        return cl_spark

    @property
    def ctl(self):
        return ctl_spark


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
    def Linker(self):
        return SQLiteLinker

    def extra_linker_args(self):
        return {"connection": self.con}

    def _get_input_name(self):
        name = f"input_alias_{self._frame_counter}"
        self._frame_counter += 1
        return name

    def convert_frame(self, df):
        name = self._get_input_name()
        df.to_sql(name, self.con, if_exists="replace")
        return name

    def load_frame_from_csv(self, path):
        return self.convert_frame(super().load_frame_from_csv(path))

    def load_frame_from_parquet(self, path):
        return self.convert_frame(super().load_frame_from_parquet(path))

    @property
    def cll(self):
        return cll_sqlite

    @property
    def cl(self):
        return cl_sqlite

    @property
    def ctl(self):
        return ctl_sqlite


class PostgresTestHelper(TestHelper):
    def __init__(self, pg_engine):
        self.engine = pg_engine
        self._frame_counter = 0

    @property
    def Linker(self):
        return PostgresLinker

    def extra_linker_args(self):
        return {"engine": self.engine}

    def _get_input_name(self):
        name = f"input_alias_{self._frame_counter}"
        self._frame_counter += 1
        return name

    def convert_frame(self, df):
        name = self._get_input_name()
        df.to_sql(name, con=self.engine, if_exists="replace")
        return name

    def load_frame_from_csv(self, path):
        return self.convert_frame(super().load_frame_from_csv(path))

    def load_frame_from_parquet(self, path):
        return self.convert_frame(super().load_frame_from_parquet(path))

    @property
    def cll(self):
        return cll_postgres

    @property
    def cl(self):
        return cl_postgres

    @property
    def ctl(self):
        return ctl_postgres

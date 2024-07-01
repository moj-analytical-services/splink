import sqlite3
from abc import ABC, abstractmethod
from collections import UserDict

import pandas as pd
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import (
    INTEGER,
    TEXT,
)

import splink.duckdb.blocking_rule_library as brl_duckdb
import splink.duckdb.comparison_level_library as cll_duckdb
import splink.duckdb.comparison_library as cl_duckdb
import splink.duckdb.comparison_template_library as ctl_duckdb
import splink.postgres.blocking_rule_library as brl_postgres
import splink.postgres.comparison_level_library as cll_postgres
import splink.postgres.comparison_library as cl_postgres
import splink.postgres.comparison_template_library as ctl_postgres
import splink.spark.blocking_rule_library as brl_spark
import splink.spark.comparison_level_library as cll_spark
import splink.spark.comparison_library as cl_spark
import splink.spark.comparison_template_library as ctl_spark
import splink.sqlite.blocking_rule_library as brl_sqlite
import splink.sqlite.comparison_level_library as cll_sqlite
import splink.sqlite.comparison_library as cl_sqlite
import splink.sqlite.comparison_template_library as ctl_sqlite
from splink.duckdb.linker import DuckDBLinker
from splink.postgres.linker import PostgresLinker
from splink.spark.linker import SparkLinker
from splink.sqlite.linker import SQLiteLinker


class TestHelper(ABC):
    @property
    @abstractmethod
    def Linker(self):
        pass

    def extra_linker_args(self):
        return {"validate_settings": False}

    @property
    def date_format(self):
        return "yyyy-mm-dd"

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

    @property
    @abstractmethod
    def brl(self):
        pass


class DuckDBTestHelper(TestHelper):
    @property
    def Linker(self):
        return DuckDBLinker

    def convert_frame(self, df):
        return df

    @property
    def date_format(self):
        return "%Y-%m-%d"

    @property
    def cll(self):
        return cll_duckdb

    @property
    def cl(self):
        return cl_duckdb

    @property
    def ctl(self):
        return ctl_duckdb

    @property
    def brl(self):
        return brl_duckdb


class SparkTestHelper(TestHelper):
    def __init__(self, spark_creator_function):
        self.spark = spark_creator_function()

    @property
    def Linker(self):
        return SparkLinker

    def extra_linker_args(self):
        core_args = super().extra_linker_args()
        return {"spark": self.spark, "num_partitions_on_repartition": 1, **core_args}

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

    @property
    def brl(self):
        return brl_spark


class SQLiteTestHelper(TestHelper):
    _frame_counter = 0

    def __init__(self):
        self.con = sqlite3.connect(":memory:")
        self._frame_counter = 0

    @property
    def Linker(self):
        return SQLiteLinker

    def extra_linker_args(self):
        core_args = super().extra_linker_args()
        return {"connection": self.con, **core_args}

    @classmethod
    def _get_input_name(cls):
        name = f"input_alias_{cls._frame_counter}"
        cls._frame_counter += 1
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

    @property
    def brl(self):
        return brl_sqlite


class PostgresTestHelper(TestHelper):
    _frame_counter = 0

    def __init__(self, pg_engine):
        if pg_engine is None:
            raise SplinkTestException("No Postgres connection found")
        self.engine = pg_engine

    @property
    def Linker(self):
        return PostgresLinker

    def extra_linker_args(self):
        core_args = super().extra_linker_args()
        return {"engine": self.engine, **core_args}

    @classmethod
    def _get_input_name(cls):
        name = f"input_alias_{cls._frame_counter}"
        cls._frame_counter += 1
        return name

    def convert_frame(self, df):
        name = self._get_input_name()
        # workaround to handle array column conversion
        # manually mark any list columns so type is handled correctly
        dtypes = {}
        for colname, values in df.items():
            # TODO: will fail if first value is null
            if isinstance(values[0], list):
                # TODO: will fail if first array is empty
                initial_array_val = values[0][0]
                if isinstance(initial_array_val, int):
                    dtypes[colname] = postgresql.ARRAY(INTEGER)
                elif isinstance(initial_array_val, str):
                    dtypes[colname] = postgresql.ARRAY(TEXT)
        df.to_sql(name, con=self.engine, if_exists="replace", dtype=dtypes)
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

    @property
    def brl(self):
        return brl_postgres


class SplinkTestException(Exception):
    pass


class LazyDict(UserDict):
    """
    LazyDict
    Like a dict, but values passed are tuples of the form (func, args)
    getting returns the result of the function
    only instantiate the result when we need it.
    Need this for handling test fixtures.
    Disallow setting/deleting entries to catch errors -
    should be effectively immutable
    """

    # write only in creation
    def __init__(self, **kwargs):
        self.data = {}
        for key, val in kwargs.items():
            self.data[key] = val

    def __getitem__(self, key):
        func, args = self.data[key]
        return func(*args)

    def __setitem__(self, key, value):
        raise SplinkTestException(
            "LazyDict does not support setting values. "
            "Did you mean to read value instead?"
        )

    def __delitem__(self, key):
        raise SplinkTestException(
            "LazyDict does not support deleting items. "
            "Did you mean to read value instead?"
        )

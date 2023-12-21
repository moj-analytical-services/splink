import sqlite3
from abc import ABC, abstractmethod
from collections import UserDict

import pandas as pd
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import (
    INTEGER,
    TEXT,
)

import splink.comparison_level_library as cll
import splink.comparison_library as cl
import splink.duckdb.blocking_rule_library as brl_duckdb
import splink.postgres.blocking_rule_library as brl_postgres
import splink.spark.blocking_rule_library as brl_spark
import splink.sqlite.blocking_rule_library as brl_sqlite
from splink.database_api import DuckDBAPI
from splink.linker import Linker
from splink.postgres.linker import PostgresLinker
from splink.spark.linker import SparkLinker
from splink.sqlite.linker import SQLiteLinker


class TestHelper(ABC):
    @property
    @abstractmethod
    def Linker(self):
        pass

    def extra_linker_args(self):
        return {}

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

    # will be able to remove this once it's all universal, but as a stepping stone
    @property
    def cll(self):
        return cll

    @property
    def cl(self):
        return cl

    # @property
    # @abstractmethod
    # def ctl(self):
    #     pass

    @property
    @abstractmethod
    def brl(self):
        pass


class DuckDBTestHelper(TestHelper):
    @property
    def Linker(self):
        return Linker

    def extra_linker_args(self):
        # create fresh api each time
        return {"database_api": DuckDBAPI()}

    def convert_frame(self, df):
        return df

    @property
    def date_format(self):
        return "%Y-%m-%d"

    # @property
    # def ctl(self):
    #     return ctl_duckdb

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
        return {"spark": self.spark, "num_partitions_on_repartition": 1}

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

    # @property
    # def ctl(self):
    #     return ctl_spark

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
        return {"connection": self.con}

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

    # @property
    # def ctl(self):
    #     return ctl_sqlite

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
        return {"engine": self.engine}

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

    # @property
    # def ctl(self):
    #     return ctl_postgres

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

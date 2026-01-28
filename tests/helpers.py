import sqlite3
from abc import ABC, abstractmethod
from collections import UserDict

import duckdb

from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker
from splink.internals.sqlite.database_api import SQLiteAPI


class TestHelper(ABC):
    @property
    def Linker(self) -> Linker:
        return Linker

    @property
    @abstractmethod
    def DatabaseAPI(self):
        pass

    def db_api_args(self):
        return {}

    # def extra_linker_args(self):
    #     # create fresh api each time
    #     return {"db_api": self.DatabaseAPI(**self.db_api_args())}

    @property
    def date_format(self):
        return "yyyy-mm-dd"

    def db_api(self):
        return self.DatabaseAPI(**self.db_api_args())

    @abstractmethod
    def convert_frame(self, df):
        pass

    def load_frame_from_csv(self, path):
        import pandas as pd

        return pd.read_csv(path)

    def load_frame_from_parquet(self, path):
        import pandas as pd

        return pd.read_parquet(path)

    @property
    def arrays_from(self) -> int:
        return 1

    def linker_with_registration(
        self, data, settings, input_table_aliases=None, **kwargs
    ):
        db_api = self.db_api()

        data_list = list(data) if isinstance(data, (list, tuple)) else [data]

        if input_table_aliases is None:
            aliases = [None] * len(data_list)
        elif isinstance(input_table_aliases, str):
            aliases = [input_table_aliases]
        else:
            aliases = list(input_table_aliases)

        sdfs = [db_api.register(d, alias) for d, alias in zip(data_list, aliases)]

        input_frames = sdfs[0] if len(sdfs) == 1 else sdfs
        return Linker(input_frames, settings, **kwargs)


class DuckDBTestHelper(TestHelper):
    def __init__(self):
        self.con = duckdb.connect()

    @property
    def DatabaseAPI(self):
        return DuckDBAPI

    def db_api_args(self):
        return {"connection": self.con}

    def convert_frame(self, df):
        return df

    def load_frame_from_csv(self, path):
        return self.con.read_csv(path)

    def load_frame_from_parquet(self, path):
        return self.con.read_parquet(path)

    @property
    def date_format(self):
        return "%Y-%m-%d"


class SparkTestHelper(TestHelper):
    def __init__(self, spark_creator_function):
        self.spark = spark_creator_function()

    @property
    def DatabaseAPI(self):
        from splink.internals.spark.database_api import SparkAPI

        return SparkAPI

    def db_api_args(self):
        return {
            "spark_session": self.spark,
            "num_partitions_on_repartition": 2,
            "break_lineage_method": "parquet",
        }

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
    def arrays_from(self) -> int:
        return 0


class SQLiteTestHelper(TestHelper):
    _frame_counter = 0

    def __init__(self):
        self.con = sqlite3.connect(":memory:")
        self._frame_counter = 0

    def db_api_args(self):
        return {"connection": self.con}

    @property
    def DatabaseAPI(self):
        return SQLiteAPI

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


class PostgresTestHelper(TestHelper):
    _frame_counter = 0

    def __init__(self, pg_engine):
        if pg_engine is None:
            raise SplinkTestException("No Postgres connection found")
        self.engine = pg_engine

    @property
    def DatabaseAPI(self):
        from splink.internals.postgres.database_api import PostgresAPI

        return PostgresAPI

    def db_api_args(self):
        return {"engine": self.engine}

    @classmethod
    def _get_input_name(cls):
        name = f"input_alias_{cls._frame_counter}"
        cls._frame_counter += 1
        return name

    def convert_frame(self, df):
        from sqlalchemy.dialects import postgresql
        from sqlalchemy.types import INTEGER, TEXT

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
        # set of keys we have accessed
        self.accessed = set()
        for key, val in kwargs.items():
            self.data[key] = val

    def __getitem__(self, key):
        func, args = self.data[key]
        self.accessed.add(key)
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

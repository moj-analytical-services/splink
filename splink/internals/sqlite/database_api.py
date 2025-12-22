import math
import sqlite3
from typing import Union

import pyarrow as pa

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import (
    SQLiteDialect,
)
from splink.internals.exceptions import SplinkException
from splink.internals.misc import is_pandas_frame

from .dataframe import SQLiteDataFrame

sql_con = sqlite3.Connection


class SQLiteAPI(DatabaseAPI[sqlite3.Cursor]):
    sql_dialect = SQLiteDialect()

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def _register_udfs(self, register_udfs: bool) -> None:
        self.con.create_function("log2", 1, math.log2)
        self.con.create_function("pow", 2, pow)
        self.con.create_function("power", 2, pow)

        # Register hash function for chunking
        # Python's hash() can return negative values, so we use abs()
        # and convert to ensure consistent behavior
        def splink_hash(s):
            return abs(hash(str(s)))

        self.con.create_function("splink_hash", 1, splink_hash)

        if register_udfs:
            try:
                from rapidfuzz.distance.DamerauLevenshtein import distance as dam_lev
                from rapidfuzz.distance.Jaro import distance as jaro
                from rapidfuzz.distance.JaroWinkler import distance as jaro_winkler
                from rapidfuzz.distance.Levenshtein import distance as levenshtein
            except ModuleNotFoundError as e:
                raise SplinkException(
                    "To use fuzzy string-matching udfs in SQLite you must install "
                    "the python package 'rapidfuzz'.  "
                    "If you do not wish to do so, and do not need to use any "
                    "fuzzy string-matching comparisons, you can use the "
                    "linker argument `register_udfs=False`.\n"
                    "See https://moj-analytical-services.github.io/splink/"
                    "topic_guides/backends.html#sqlite for more information"
                ) from e
        else:
            return

        def wrap_func_with_str(func):
            def wrapped_func(str_l, str_r):
                return func(str(str_l), str(str_r))

            return wrapped_func

        funcs_to_register = {
            "levenshtein": levenshtein,
            "damerau_levenshtein": dam_lev,
            "jaro_winkler": jaro_winkler,
            "jaro": jaro,
        }

        for sql_name, func in funcs_to_register.items():
            self.con.create_function(sql_name, 2, wrap_func_with_str(func))

    def __init__(
        self, connection: Union[str, sql_con] = ":memory:", register_udfs: bool = True
    ):
        super().__init__()

        if isinstance(connection, str):
            connection = sqlite3.connect(connection)
        self.con = connection
        self.con.row_factory = self.dict_factory
        self._register_udfs(register_udfs)

    @staticmethod
    def _arrow_to_sqlite_type(field: pa.Field) -> str:
        t = field.type

        if pa.types.is_integer(t) or pa.types.is_boolean(t):
            return "INTEGER"
        if pa.types.is_floating(t):
            return "REAL"
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            return "BLOB"
        # string, timestamp, decimal, anything else all map to TEXT
        return "TEXT"

    def _register_pyarrow_table(self, table: pa.Table, table_name: str) -> None:
        cur = self.con.cursor()

        cols = table.schema
        col_defs = ", ".join(
            f"{field.name} {self._arrow_to_sqlite_type(field)}" for field in cols
        )
        cur.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")

        rows = zip(*(table.column(i).to_pylist() for i in range(table.num_columns)))

        placeholders = ", ".join("?" * table.num_columns)
        cur.executemany(f"INSERT INTO data VALUES ({placeholders})", rows)

        self.con.commit()

    def _table_registration(self, input, table_name):
        if is_pandas_frame(input):
            # Will error if an invalid data type is passed
            input.to_sql(
                table_name,
                self.con,
                index=False,
                if_exists="replace",
            )
        if isinstance(input, dict):
            input = pa.Table.from_pydict(input)
        elif isinstance(input, list):
            input = pa.Table.from_pylist(input)

    def table_to_splink_dataframe(self, templated_name, physical_name):
        return SQLiteDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        rec = self._execute_sql_against_backend(sql).fetchone()
        if not rec:
            return False
        else:
            return True

    def _execute_sql_against_backend(self, final_sql: str) -> sqlite3.Cursor:
        return self.con.execute(final_sql)

    def _create_or_replace_temp_view(self, name: str, physical: str) -> None:
        self._execute_sql_against_backend(f"DROP VIEW IF EXISTS {name}")
        self._execute_sql_against_backend(
            f"CREATE TEMP VIEW {name} AS SELECT * FROM {physical}"
        )

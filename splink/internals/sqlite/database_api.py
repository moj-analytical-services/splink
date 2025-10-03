import math
import sqlite3
from typing import Union

import pandas as pd

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import (
    SQLiteDialect,
)
from splink.internals.exceptions import SplinkException

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

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Will error if an invalid data type is passed
        input.to_sql(
            table_name,
            self.con,
            index=False,
            if_exists="replace",
        )

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

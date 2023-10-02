from __future__ import annotations

import logging
import sqlite3
from math import log2, pow

import pandas as pd

from ..exceptions import SplinkException
from ..input_column import InputColumn
from ..linker import Linker
from ..misc import ensure_is_list
from ..splink_dataframe import SplinkDataFrame
from ..unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteDataFrame(SplinkDataFrame):
    linker: SQLiteLinker

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        PRAGMA table_info({self.physical_name});
        """
        pragma_result = self.linker.con.execute(sql).fetchall()
        cols = [r["name"] for r in pragma_result]

        return [InputColumn(c, sql_dialect="sqlite") for c in cols]

    def validate(self):
        if type(self.physical_name) is not str:
            raise ValueError(
                f"{self.df_name} is not a string dataframe.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of the "
                " sqlite table."
            )

        sql = f"""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='{self.physical_name}';
        """

        res = self.linker.con.execute(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the sqlite db provided.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of a "
                " sqlite table that exists in the provided db."
            )

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        drop_sql = f"""
        DROP TABLE IF EXISTS {self.physical_name}"""
        cur = self.linker.con.cursor()
        cur.execute(drop_sql)

    def as_record_dict(self, limit=None):
        sql = f"""
        select *
        from {self.physical_name}
        """
        if limit:
            sql += f" limit {limit}"
        sql += ";"
        cur = self.linker.con.cursor()
        return cur.execute(sql).fetchall()


class SQLiteLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict: dict | str = None,
        connection=":memory:",
        set_up_basic_logging=True,
        input_table_aliases: str | list = None,
        validate_settings: bool = True,
        register_udfs=True,
    ):
        self._sql_dialect_ = "sqlite"

        if isinstance(connection, str):
            connection = sqlite3.connect(connection)
        self.con = connection
        self.con.row_factory = dict_factory
        # maths functions not always available by default depending on system
        self.con.create_function("log2", 1, log2)
        self.con.create_function("pow", 2, pow)
        self.con.create_function("power", 2, pow)
        if register_udfs:
            self._register_udfs()

        input_tables = ensure_is_list(input_table_or_tables)
        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )
        accepted_df_dtypes = pd.DataFrame

        super().__init__(
            input_tables,
            settings_dict,
            accepted_df_dtypes,
            set_up_basic_logging,
            input_table_aliases=input_aliases,
            validate_settings=validate_settings,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return SQLiteDataFrame(templated_name, physical_name, self)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"""
        create table {physical_name}
        as
        {sql}
        """
        self._log_and_run_sql_execution(sql, templated_name, physical_name)

        output_obj = self._table_to_splink_dataframe(templated_name, physical_name)
        return output_obj

    def _run_sql_execution(
        self, final_sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        return self.con.execute(final_sql)

    def register_table(self, input, table_name, overwrite=False):
        # If the user has provided a table name, return it as a SplinkDataframe
        if isinstance(input, str):
            return self._table_to_splink_dataframe(table_name, input)

        # Check if table name is already in use
        exists = self._table_exists_in_database(table_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{table_name}' already exists in database. "
                    "Please use the 'overwrite' argument if you wish to overwrite"
                )
            else:
                self._delete_table_from_database(table_name)

        self._table_registration(input, table_name)
        return self._table_to_splink_dataframe(table_name, table_name)

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

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        if seed:
            raise NotImplementedError(
                "SQLite does not support seeds in random ",
                "samples. Please remove the `seed` parameter.",
            )

        sample_size = int(sample_size)

        if unique_id is None:
            # unique_id col, with source_dataset column if needed to disambiguate
            unique_id_cols = self._settings_obj._unique_id_input_columns
            unique_id = _composite_unique_id_from_nodes_sql(unique_id_cols)
        if table is None:
            table = "__splink__df_concat_with_tf"
        return (
            f"WHERE {unique_id} IN ("
            f"    SELECT {unique_id} FROM {table}"
            f"    ORDER BY RANDOM() LIMIT {sample_size}"
            f")"
        )

    @property
    def _infinity_expression(self):
        return "'infinity'"

    def _table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        rec = self.con.execute(sql).fetchone()
        if not rec:
            return False
        else:
            return True

    def _delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self.con.execute(drop_sql)

    def _register_udfs(self):
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
                "fuzzy string-matching comparisons, you can use the linker argument "
                "`register_udfs=False`.\n"
                "See https://moj-analytical-services.github.io/splink/"
                "topic_guides/backends.html#sqlite for more information"
            ) from e

        def wrap_func_with_str(func):
            def wrapped_func(str_l, str_r):
                return func(str(str_l), str(str_r))

            return wrapped_func

        # name in SQL : python function
        funcs_to_register = {
            "levenshtein": levenshtein,
            "damerau_levenshtein": dam_lev,
            "jaro_winkler": jaro_winkler,
            "jaro": jaro,
        }

        for sql_name, func in funcs_to_register.items():
            self.con.create_function(sql_name, 2, wrap_func_with_str(func))

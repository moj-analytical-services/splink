from __future__ import annotations

import logging

import pandas as pd
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine

from ..input_column import InputColumn
from ..linker import Linker
from ..misc import ensure_is_list
from ..splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class PostgresDataFrame(SplinkDataFrame):
    linker: PostgresLinker

    def __init__(self, df_name, physical_name, linker):
        super().__init__(df_name, physical_name, linker)
        self._db_schema = "splink"
        self.physical_name = f"{self.physical_name}"

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{self.physical_name}';
        """
        result = self.linker.con.cursor(cursor_factory=RealDictCursor)
        result.execute(sql)
        cols = [r["column_name"] for r in result.fetchall()]

        return [InputColumn(c, sql_dialect="postgres") for c in cols]

    def validate(self):
        if type(self.physical_name) is not str:
            raise ValueError(
                f"{self.df_name} is not a string dataframe.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of the "
                " postgres table."
            )

        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{self.physical_name}';
        """

        result = self.linker.con.cursor(cursor_factory=RealDictCursor)
        result.execute(sql)
        res = result.fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the postgres db provided.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of a "
                " postgres table that exists in the provided db."
            )

    def drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        drop_sql = f"""
        DROP TABLE IF EXISTS {self.physical_name};"""
        cur = self.linker.con.cursor()
        cur.execute(drop_sql)
        self.linker.con.commit()

    def as_record_dict(self, limit=None):
        sql = f"""
        SELECT *
        FROM {self.physical_name}
        """
        if limit:
            sql += f" LIMIT {limit}"
        sql += ";"
        cur = self.linker.con.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        return cur.fetchall()


class PostgresLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        connection=None,
        set_up_basic_logging=True,
        input_table_aliases: str | list = None,
    ):
        self._sql_dialect_ = "postgres"
        if connection is None:
            raise ValueError("You must supply a valid postgres connection to create a PostgresLinker")
        self.con = connection

        input_tables = ensure_is_list(input_table_or_tables)
        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )
        accepted_df_dtypes = pd.DataFrame
        self._db_schema = "splink"

        # Create log2 function in database
        self._create_log2_function()

        # Create splink schema
        self._create_splink_schema()

        super().__init__(
            input_tables,
            settings_dict,
            accepted_df_dtypes,
            set_up_basic_logging,
            input_table_aliases=input_aliases,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return PostgresDataFrame(templated_name, physical_name, self)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"CREATE TABLE {physical_name} AS {sql}"
        self._log_and_run_sql_execution(sql, templated_name, physical_name)

        output_obj = self._table_to_splink_dataframe(templated_name, physical_name)
        return output_obj

    def _run_sql_execution(
        self, final_sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        cur = self.con.cursor()
        cur.execute(final_sql)
        self.con.commit()
        cur.close()

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        def connection_fairy():
            return self.con

        engine = create_engine("postgresql://", creator=connection_fairy)

        # Will error if an invalid data type is passed
        input.to_sql(
            table_name,
            con=engine,
            index=False,
            if_exists="replace",
            schema=self._db_schema,
        )

    def register_table(self, input, table_name, overwrite=True):
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

    def _random_sample_sql(self, proportion, sample_size, seed=None):
        if proportion == 1.0:
            return ""
        if seed is None:
            seed = "random()"

        sample_size = int(sample_size)
        return f"""
                WHERE unique_id IN (
                    SELECT unique_id
                    FROM __splink__df_concat_with_tf
                    ORDER BY {seed}
                    LIMIT {sample_size}
                )
            """

    @property
    def _infinity_expression(self):
        return "'infinity'"

    def _table_exists_in_database(self, table_name):
        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """

        cur = self.con.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rec = cur.fetchone()
        cur.close()
        return rec is not None

    def _delete_table_from_database(self, name):
        drop_sql = f"DROP TABLE IF EXISTS {name};"
        cur = self.con.cursor()
        cur.execute(drop_sql)
        self.con.commit()
        cur.close()

    def _create_log2_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION log2(n float8)
        RETURNS float8 AS $$
        SELECT log(2.0, n::numeric)::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        cur = self.con.cursor()
        cur.execute(sql)
        self.con.commit()
        cur.close()

    def _create_splink_schema(self):
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self._db_schema};
        SET search_path TO {self._db_schema},public;
        """
        cur = self.con.cursor()
        cur.execute(sql)
        self.con.commit()
        cur.close()

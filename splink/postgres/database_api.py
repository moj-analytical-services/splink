import logging
from typing import Any, List, Union

import duckdb
import pandas as pd
from sqlalchemy import CursorResult, text
from sqlalchemy.engine import Engine

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import (
    PostgresDialect,
)
from splink.internals.misc import (
    ensure_is_list,
)

from .dataframe import PostgresDataFrame

logger = logging.getLogger(__name__)


class PostgresAPI(DatabaseAPI[CursorResult[Any]]):
    sql_dialect = PostgresDialect()

    def __init__(
        self,
        engine: Engine,
        schema: str = "splink",
        other_schemas_to_search: Union[str, List[str]] = [],
    ):
        super().__init__()
        if not isinstance(engine, Engine):
            raise ValueError(
                "You must supply a sqlalchemy engine to create a PostgresAPI."
            )

        self._engine = engine
        self._db_schema = schema
        self._create_splink_schema(other_schemas_to_search)

        self._register_custom_functions()
        self._register_extensions()

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Using Duckdb to insert the data ensures the correct datatypes
        # and faster insertion (duckdb>=0.9.2)
        con = duckdb.connect()

        try:
            con.execute("INSTALL postgres;")

            url = self._engine.url

            pg_con_str = (
                f"postgresql://{url.username}:{url.password}"
                f"@{url.host}:{url.port}/{url.database}"
            )

            con.execute(f"ATTACH '{pg_con_str}' AS pg_db (TYPE postgres);")
            con.register("temp_df", input)
            con.execute(
                f"CREATE OR REPLACE TABLE pg_db.{self._db_schema}.{table_name} "
                "AS SELECT * FROM temp_df;"
            )

        except (duckdb.HTTPException, duckdb.BinderException):
            input.to_sql(
                table_name,
                con=self._engine,
                index=False,
                if_exists="replace",
                schema=self._db_schema,
            )

    def table_to_splink_dataframe(self, templated_name, physical_name):
        return PostgresDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{table_name}';
        """

        rec = self._execute_sql_against_backend(sql).mappings().all()
        return len(rec) > 0

    def _execute_sql_against_backend(
        self, final_sql: str, templated_name: str = None, physical_name: str = None
    ) -> CursorResult[Any]:
        with self._engine.begin() as con:
            res = con.execute(text(final_sql))
        return res

    # postgres udf registrations:
    def _create_log2_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION log2(n float8)
        RETURNS float8 AS $$
        SELECT log(2.0, n::numeric)::float8;
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._execute_sql_against_backend(sql)

    def _extend_round_function(self):
        # extension of round to double
        sql = """
        CREATE OR REPLACE FUNCTION round(n float8, dp integer)
        RETURNS numeric AS $$
        SELECT round(n::numeric, dp);
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._execute_sql_against_backend(sql)

    def _create_try_cast_date_function(self):
        # postgres to_date will give an error if the date can't be parsed
        # to be consistent with other backends we instead create a version
        # which instead returns NULL, allowing us more flexibility
        sql = """
        CREATE OR REPLACE FUNCTION try_cast_date(date_string text, format text)
        RETURNS date AS $func$
        BEGIN
            BEGIN
                RETURN to_date(date_string, format);
            EXCEPTION WHEN OTHERS THEN
                RETURN NULL;
            END;
        END
        $func$ LANGUAGE plpgsql IMMUTABLE;
        """
        self._execute_sql_against_backend(sql)

    def _create_try_cast_timestamp_function(self):
        # postgres to_timestamp will give an error if the timestamp can't be parsed
        # to be consistent with other backends we instead create a version
        # which instead returns NULL, allowing us more flexibility
        sql = """
        CREATE OR REPLACE FUNCTION try_cast_timestamp(timestamp_str text, format text)
        RETURNS timestamp AS $func$
        BEGIN
            BEGIN
                RETURN to_timestamp(timestamp_str, format);
            EXCEPTION WHEN OTHERS THEN
                RETURN NULL;
            END;
        END
        $func$ LANGUAGE plpgsql IMMUTABLE;
        """
        self._execute_sql_against_backend(sql)

    def _create_array_intersect_function(self):
        sql = """
        CREATE OR REPLACE FUNCTION array_intersect(x anyarray, y anyarray)
        RETURNS anyarray AS $$
        SELECT ARRAY( SELECT DISTINCT * FROM UNNEST(x) WHERE UNNEST = ANY(y) )
        $$ LANGUAGE SQL IMMUTABLE;
        """
        self._execute_sql_against_backend(sql)

    def _register_custom_functions(self):
        # if people have issues with permissions we can allow these to be optional
        # need for predict_from_comparison_vectors_sql (could adjust)
        self._create_log2_function()
        # need for date-casting
        self._create_try_cast_date_function()
        self._create_try_cast_timestamp_function()
        # need for array_intersect levels
        self._create_array_intersect_function()
        # extension of round to handle doubles - used in unlinkables
        self._extend_round_function()

    def _register_extensions(self):
        sql = """
        CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
        """
        self._execute_sql_against_backend(sql)

    def _create_splink_schema(self, other_schemas_to_search):
        other_schemas_to_search = ensure_is_list(other_schemas_to_search)
        # always search _db_schema first and public last
        schemas_to_search = [self._db_schema] + other_schemas_to_search + ["public"]
        search_path = ",".join(schemas_to_search)
        sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self._db_schema};
        SET search_path TO {search_path};
        """
        self._execute_sql_against_backend(sql)

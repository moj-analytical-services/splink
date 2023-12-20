import logging

import duckdb
import pandas as pd
import sqlglot

from .cache_dict_with_logging import CacheDictWithLogging
from .dialects import DuckDBDialect
from .duckdb.duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    validate_duckdb_connection,
)
from .duckdb.linker import DuckDBDataFrame
from .exceptions import SplinkException
from .logging_messages import execute_sql_logging_message_info, log_sql
from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


class DatabaseAPI:
    def __init__(self):
        self._intermediate_table_cache: dict = CacheDictWithLogging()

    def _log_and_run_sql_execution(
        self, final_sql: str, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        """Log the sql, then call _run_sql_execution(), wrapping any errors"""
        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(final_sql))
        try:
            return self._run_sql_execution(final_sql, templated_name, physical_name)
        except Exception as e:
            # Parse our SQL through sqlglot to pretty print
            try:
                final_sql = sqlglot.parse_one(
                    final_sql,
                    read=self._sql_dialect,
                ).sql(pretty=True)
                # if sqlglot produces any errors, just report the raw SQL
            except Exception:
                pass

            raise SplinkException(
                f"Error executing the following sql for table "
                f"`{templated_name}`({physical_name}):\n{final_sql}"
                f"\n\nError was: {e}"
            ) from e

    # should probably also be responsible for cache
    # TODO: stick this in a cache-api that lives on this

    def _remove_splinkdataframe_from_cache(self, splink_dataframe: SplinkDataFrame):
        keys_to_delete = set()
        for key, df in self._intermediate_table_cache.items():
            if df.physical_name == splink_dataframe.physical_name:
                keys_to_delete.add(key)

        for k in keys_to_delete:
            del self._intermediate_table_cache[k]


class DuckDBAPI(DatabaseAPI):
    sql_dialect = DuckDBDialect()

    def __init__(self, connection: str = ":memory:", output_schema: str = None,):
        super().__init__()
        validate_duckdb_connection(connection, logger)

        if isinstance(connection, str):
            con_lower = connection.lower()
        if isinstance(connection, duckdb.DuckDBPyConnection):
            con = connection
        elif con_lower == ":memory:":
            con = duckdb.connect(database=connection)
        elif con_lower == ":temporary:":
            con = create_temporary_duckdb_connection(self)
        else:
            con = duckdb.connect(database=connection)

        self._con = con

        if output_schema:
            self._con.execute(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

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
                self._con.unregister(table_name)

        self._table_registration(input, table_name)
        return self._table_to_splink_dataframe(table_name, table_name)

    def _table_registration(self, input, table_name) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Registration errors will automatically
        # occur if an invalid data type is passed as an argument
        self._con.register(table_name, input)

    def table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> DuckDBDataFrame:
        # TODO: this is a slight lie atm,
        # as DuckDBDataFrame thinks we are passing a Linker
        return DuckDBDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        # From duckdb 0.5.0, duckdb will raise a CatalogException
        # which does not exist in 0.4.0 or before

        try:
            from duckdb import CatalogException

            error = (RuntimeError, CatalogException)
        except ImportError:
            error = RuntimeError

        try:
            self._con.execute(sql)
        except error:
            return False
        return True

    def execute_sql_against_backend(
        self, sql: str, templated_name: str, physical_name: str
    ):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"""
        CREATE TABLE {physical_name}
        AS
        ({sql})
        """
        self._log_and_run_sql_execution(sql, templated_name, physical_name)

        return DuckDBDataFrame(templated_name, physical_name, self)

    def _run_sql_execution(self, final_sql, templated_name, physical_name):
        self._con.execute(final_sql)

    def _delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self._con.execute(drop_sql)

    @property
    def accepted_df_dtypes(self):
        accepted_df_dtypes = [pd.DataFrame]
        try:
            # If pyarrow is installed, add to the accepted list
            import pyarrow as pa

            accepted_df_dtypes.append(pa.lib.Table)
        except ImportError:
            pass
        return accepted_df_dtypes

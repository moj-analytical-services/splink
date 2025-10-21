from __future__ import annotations

import logging
from typing import Union

import duckdb
import pandas as pd

from splink.internals.database_api import AcceptableInputTableType, DatabaseAPI
from splink.internals.dialects import (
    DuckDBDialect,
)

from .dataframe import DuckDBDataFrame
from .duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    validate_duckdb_connection,
)

logger = logging.getLogger(__name__)


class DuckDBAPI(DatabaseAPI[duckdb.DuckDBPyRelation]):
    sql_dialect = DuckDBDialect()

    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str = None,
    ):
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
            self._execute_sql_against_backend(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

    def delete_table_from_database(self, name: str) -> None:
        # If the table is in fact a pandas dataframe that's been registered using
        # duckdb con.register() then DROP TABLE will fail with
        # Catalog Error: x is of type View
        try:
            drop_sql = f"DROP TABLE IF EXISTS {name}"
            self._execute_sql_against_backend(drop_sql)
        except duckdb.CatalogException:
            drop_sql = f"DROP VIEW IF EXISTS {name}"
            self._execute_sql_against_backend(drop_sql)

    def _table_registration(
        self, input: AcceptableInputTableType, table_name: str
    ) -> None:
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            try:
                # pyarrow preserves types better than pandas
                import pyarrow as pa

                input = pa.Table.from_pylist(input)
            except ImportError:
                input = pd.DataFrame.from_records(input)

        self._con.register(table_name, input)

    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> DuckDBDataFrame:
        return DuckDBDataFrame(templated_name, physical_name, self)

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"
        from duckdb import CatalogException

        try:
            self._execute_sql_against_backend(sql)
        except CatalogException:
            return False
        return True

    def _execute_sql_against_backend(self, final_sql: str) -> duckdb.DuckDBPyRelation:
        return self._con.sql(final_sql)

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

from __future__ import annotations

import logging
from os import PathLike
from typing import Literal, Union

import duckdb

from splink.internals.database_api import AcceptableInputTableType, DatabaseAPI
from splink.internals.dialects import (
    DuckDBDialect,
)
from splink.internals.misc import to_pyarrow_if_list_tuple_or_dict

from .dataframe import DuckDBDataFrame
from .duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    validate_duckdb_connection,
)
from .parquet_materialisation import _ParquetMaterialiser
from .parquet_write_options import ParquetWriteOptions

logger = logging.getLogger(__name__)


class DuckDBAPI(DatabaseAPI[duckdb.DuckDBPyRelation]):
    sql_dialect = DuckDBDialect()

    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str | None = None,
        *,
        materialisation: Literal["table", "parquet"] = "table",
        materialisation_dir: str | PathLike[str] | None = None,
        parquet_materialisation_options: ParquetWriteOptions | None = None,
    ):
        """
        Parquet mode writes SQL results directly to parquet files rather
        than DuckDB storage.  This can be significantly faster for
        very large linkages if you're using an in-memory connection
        and you hit memory limits.
        """
        if materialisation not in ("table", "parquet"):
            raise ValueError("materialisation must be 'table' or 'parquet'")
        self._materialisation: Literal["table", "parquet"] = materialisation
        self._parquet_materialiser: _ParquetMaterialiser | None = None

        if self._materialisation == "table":
            if (
                materialisation_dir is not None
                or parquet_materialisation_options is not None
            ):
                raise ValueError(
                    "Parquet directory/options require materialisation='parquet'"
                )
        else:
            if materialisation_dir is None:
                raise ValueError("materialisation_dir is required for Parquet mode")
            if parquet_materialisation_options is not None and not isinstance(
                parquet_materialisation_options, ParquetWriteOptions
            ):
                raise TypeError(
                    "parquet_materialisation_options must be ParquetWriteOptions"
                )
            self._parquet_materialiser = _ParquetMaterialiser(
                materialisation_dir,
                parquet_materialisation_options or ParquetWriteOptions(),
            )
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

    @property
    def duckdb_con(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def _get_parquet_materialiser(self) -> _ParquetMaterialiser:
        materialiser = self._parquet_materialiser
        if materialiser is None:
            raise RuntimeError("Parquet materialisation was not initialised")
        return materialiser

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

        if self._materialisation == "parquet":
            materialiser = self._get_parquet_materialiser()
            materialiser.delete_backing_files(name)

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:

        # In parquet mode, rather than 'create table as'
        # we need to remove any parquet files and then
        # 'COPY ({query}) to {dir} FORMAT PARQUET
        # instead of the normal 'DROP TABLE IF EXISTS'
        # then 'CREATE TABLE {name} as {query}'
        if self._materialisation == "parquet":
            materialiser = self._get_parquet_materialiser()

            self.delete_table_from_database(physical_name)
            return materialiser.prepare_sql(sql, physical_name)

        return super()._setup_for_execute_sql(sql, physical_name)

    def _cleanup_for_execute_sql(self, table, templated_name, physical_name):
        if self._materialisation == "table":
            return super()._cleanup_for_execute_sql(
                table, templated_name, physical_name
            )

        # In parquet mode rather than just returning the
        # table as a splink dataframe we
        # 'create view {name} as select * from read_parquet()'
        # and return that view as a Splink dataframe
        materialiser = self._get_parquet_materialiser()
        self._execute_sql_against_backend(materialiser.view_sql(physical_name))
        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        materialiser.complete(physical_name)
        return output_df

    def _table_registration(
        self, input: AcceptableInputTableType, table_name: str
    ) -> None:
        input = to_pyarrow_if_list_tuple_or_dict(input)

        self._con.register(table_name, input)

    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> DuckDBDataFrame:
        return DuckDBDataFrame(templated_name, physical_name, self)

    def _load_from_csv(self, path: str) -> str:
        tn = self._new_input_table_name()
        self._con.execute(f"CREATE TABLE {tn} AS FROM read_csv_auto('{path}')")
        return tn

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

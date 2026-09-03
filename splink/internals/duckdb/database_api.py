from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

import duckdb

from splink.internals.database_api import AcceptableInputTableType, DatabaseAPI
from splink.internals.dialects import (
    DuckDBDialect,
)
from splink.internals.misc import ascii_uid, to_pyarrow_if_list_tuple_or_dict
from splink.internals.pipeline import CTEPipeline

from .dataframe import DuckDBDataFrame
from .duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    validate_duckdb_connection,
)
from .parquet_sink import DuckDBCompression

logger = logging.getLogger(__name__)


class DuckDBAPI(DatabaseAPI[duckdb.DuckDBPyRelation]):
    sql_dialect = DuckDBDialect()

    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str | None = None,
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

    @property
    def duckdb_con(self) -> duckdb.DuckDBPyConnection:
        return self._con

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
        input = to_pyarrow_if_list_tuple_or_dict(input)

        self._con.register(table_name, input)

    def table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> DuckDBDataFrame:
        return DuckDBDataFrame(templated_name, physical_name, self)

    def _sql_pipeline_to_parquet(
        self,
        pipeline: CTEPipeline,
        *,
        output_path: Path,
        compression: DuckDBCompression,
        row_group_size: int | None,
        file_size_bytes: str | None,
    ) -> DuckDBDataFrame:
        sql = pipeline.generate_cte_pipeline_sql()
        output_path.mkdir(parents=True, exist_ok=True)
        templated_name = pipeline.output_table_name
        physical_name = f"{templated_name}_{ascii_uid(8)}"

        options = [
            "FORMAT PARQUET",
            "PER_THREAD_OUTPUT TRUE",
            f"COMPRESSION {compression.upper()}",
        ]
        if row_group_size is not None:
            options.append(f"ROW_GROUP_SIZE {row_group_size}")
        if file_size_bytes is not None:
            escaped_file_size = file_size_bytes.replace("'", "''")
            options.append(f"FILE_SIZE_BYTES '{escaped_file_size}'")

        escaped_output_path = str(output_path).replace("'", "''")
        copy_sql = f"COPY (\n{sql}\n) TO '{escaped_output_path}' ({', '.join(options)})"
        self._log_and_run_sql_execution(copy_sql, templated_name, physical_name)

        return self._parquet_files_to_splink_dataframe(
            output_path=output_path,
            parquet_glob=output_path / "*.parquet",
            templated_name=templated_name,
            sql_used_to_create=sql,
        )

    def _parquet_files_to_splink_dataframe(
        self,
        *,
        output_path: Path,
        parquet_glob: Path,
        templated_name: str,
        sql_used_to_create: str | None = None,
    ) -> DuckDBDataFrame:
        physical_name = f"{templated_name}_{ascii_uid(8)}"
        escaped_parquet_glob = str(parquet_glob).replace("'", "''")
        view_sql = (
            f"CREATE VIEW {physical_name} AS "
            f"SELECT * FROM read_parquet('{escaped_parquet_glob}')"
        )
        self._log_and_run_sql_execution(view_sql, templated_name, physical_name)

        output_df = self.table_to_splink_dataframe(templated_name, physical_name)
        output_df.created_by_splink = True
        output_df.sql_used_to_create = sql_used_to_create or view_sql
        output_df.metadata["parquet_path"] = str(output_path)
        self._created_tables.add(physical_name)
        self._intermediate_table_cache.executed_queries.append(output_df)
        return output_df

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

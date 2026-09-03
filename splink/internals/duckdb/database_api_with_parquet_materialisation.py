from __future__ import annotations

import shutil
from os import PathLike
from pathlib import Path
from typing import Union

import duckdb

from .database_api import DuckDBAPI
from .dataframe import DuckDBDataFrame


class DuckDBAPIWithParquetMaterialisation(DuckDBAPI):
    """Store Splink materialisations as Parquet-backed views.

    ``parquet_materialisation_dir`` is scratch storage owned by this API. Individual
    child directories are removed with their corresponding Splink tables; abandoned
    contents may be removed manually after a crashed process.
    """

    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str | None = None,
        *,
        parquet_materialisation_dir: str | PathLike[str],
        compression: str = "zstd",
    ):
        super().__init__(connection=connection, output_schema=output_schema)
        self.parquet_materialisation_dir = (
            Path(parquet_materialisation_dir).expanduser().resolve()
        )
        self.parquet_materialisation_dir.mkdir(parents=True, exist_ok=True)
        self.compression = compression

    def _parquet_path_for_physical_name(self, physical_name: str) -> Path:
        return self.parquet_materialisation_dir / physical_name

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        self.delete_table_from_database(physical_name)

        parquet_path = self._parquet_path_for_physical_name(physical_name)
        parquet_path.mkdir(parents=True, exist_ok=True)
        escaped_path = str(parquet_path).replace("'", "''")

        return f"""
            COPY (
                {sql}
            )
            TO '{escaped_path}'
            (
                FORMAT PARQUET,
                PER_THREAD_OUTPUT TRUE,
                COMPRESSION {self.compression.upper()}
            )
        """

    def _cleanup_for_execute_sql(
        self, table: duckdb.DuckDBPyRelation, templated_name: str, physical_name: str
    ) -> DuckDBDataFrame:
        parquet_glob = self._parquet_path_for_physical_name(physical_name) / "*.parquet"
        escaped_glob = str(parquet_glob).replace("'", "''")
        view_sql = (
            f"CREATE VIEW {physical_name} AS "
            f"SELECT * FROM read_parquet('{escaped_glob}')"
        )
        self._log_and_run_sql_execution(view_sql, templated_name, physical_name)

        return self.table_to_splink_dataframe(templated_name, physical_name)

    def delete_table_from_database(self, name: str) -> None:
        super().delete_table_from_database(name)

        parquet_path = self._parquet_path_for_physical_name(name)
        if parquet_path.exists():
            shutil.rmtree(parquet_path)

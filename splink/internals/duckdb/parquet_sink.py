from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from splink.internals.misc import ascii_uid
from splink.internals.pipeline import CTEPipeline

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPI
    from splink.internals.splink_dataframe import SplinkDataFrame


DuckDBCompression = Literal[
    "uncompressed", "brotli", "snappy", "lz4", "lz4_raw", "gzip", "zstd"
]


@dataclass(frozen=True)
class ParquetSink:
    """Write DuckDB prediction results directly to a Parquet dataset directory."""

    path: str | Path
    compression: str = "zstd"
    row_group_size: int | None = 1_000_000
    file_size_bytes: str | None = "512MB"
    overwrite: bool = False

    def validate_db_api(self, db_api: DatabaseAPI[Any]) -> None:
        from .database_api import DuckDBAPI

        if not isinstance(db_api, DuckDBAPI):
            raise TypeError(
                "ParquetSink is currently supported only with the DuckDB backend."
            )

    def prepare(self) -> None:
        output_path = Path(self.path)
        if output_path.exists():
            if not self.overwrite:
                raise FileExistsError(
                    f"Parquet output directory already exists: {output_path}"
                )
            if output_path.is_dir():
                shutil.rmtree(output_path)
            else:
                output_path.unlink()

        output_path.mkdir(parents=True, exist_ok=True)

    def for_chunk(
        self, left_chunk: tuple[int, int], right_chunk: tuple[int, int]
    ) -> ParquetSink:
        left_number, left_total = left_chunk
        right_number, right_total = right_chunk
        chunk_path = (
            Path(self.path)
            / f"chunk_l{left_number}_of_{left_total}_r{right_number}_of_{right_total}"
        )
        return ParquetSink(
            path=chunk_path,
            compression=self.compression,
            row_group_size=self.row_group_size,
            file_size_bytes=self.file_size_bytes,
            overwrite=self.overwrite,
        )

    def write_pipeline(
        self, db_api: DatabaseAPI[Any], pipeline: CTEPipeline
    ) -> SplinkDataFrame:
        self.validate_db_api(db_api)
        if db_api.debug_mode:
            raise ValueError("ParquetSink is not supported when debug_mode is enabled.")

        self.prepare()
        sql = pipeline.generate_cte_pipeline_sql()
        output_path = Path(self.path)
        relation = db_api.duckdb_con.sql(sql)
        relation.write_parquet(
            str(output_path),
            compression=cast(DuckDBCompression, self.compression),
            per_thread_output=True,
            row_group_size=self.row_group_size,
            file_size_bytes=self.file_size_bytes,
            overwrite=self.overwrite,
        )

        return self._create_parquet_view(
            db_api,
            pipeline.output_table_name,
            output_path / "*.parquet",
            sql,
        )

    def combine_chunks(self, db_api: DatabaseAPI[Any]) -> SplinkDataFrame:
        self.validate_db_api(db_api)
        if db_api.debug_mode:
            raise ValueError("ParquetSink is not supported when debug_mode is enabled.")

        output_path = Path(self.path)
        view_sql = self._parquet_view_sql(
            output_path / "*" / "*.parquet"
        )
        return self._create_parquet_view(
            db_api,
            "__splink__df_predict",
            output_path / "*" / "*.parquet",
            view_sql,
        )

    def _create_parquet_view(
        self,
        db_api: DatabaseAPI[Any],
        templated_name: str,
        parquet_glob: Path,
        sql_used_to_create: str,
    ) -> SplinkDataFrame:
        physical_name = f"{templated_name}_{ascii_uid(8)}"
        view_sql = self._parquet_view_sql(parquet_glob)
        db_api._execute_sql_against_backend(
            f"CREATE VIEW {physical_name} AS {view_sql}"
        )

        output_df = db_api.table_to_splink_dataframe(templated_name, physical_name)
        output_df.created_by_splink = True
        output_df.sql_used_to_create = sql_used_to_create
        output_df.metadata["parquet_path"] = str(self.path)
        db_api._created_tables.add(physical_name)
        db_api._intermediate_table_cache.executed_queries.append(output_df)
        return output_df

    @staticmethod
    def _parquet_view_sql(parquet_glob: Path) -> str:
        path = str(parquet_glob).replace("'", "''")
        return f"SELECT * FROM read_parquet('{path}')"

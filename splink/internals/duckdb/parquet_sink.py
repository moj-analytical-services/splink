from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.splink_dataframe import SplinkDataFrame


DuckDBCompression = Literal[
    "uncompressed", "brotli", "snappy", "lz4", "lz4_raw", "gzip", "zstd"
]


@dataclass(frozen=True)
class ParquetSink:
    """Write DuckDB prediction results directly to a Parquet dataset directory."""

    path: str | Path
    compression: DuckDBCompression = "zstd"
    row_group_size: int | None = None
    file_size_bytes: str | None = None
    overwrite: bool = False

    def validate_db_api(self, db_api: Any) -> None:
        from .database_api import DuckDBAPI

        if not isinstance(db_api, DuckDBAPI):
            raise TypeError(
                "ParquetSink is currently supported only with the DuckDB backend."
            )
        if db_api.debug_mode:
            raise ValueError("ParquetSink is not supported when debug_mode is enabled.")

    def prepare_root(self) -> None:
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

    def prepare(self) -> None:
        """Prepare this sink's output directory.

        This is retained as a convenience alias for ``prepare_root``. Child sinks
        returned by ``for_chunk`` must not be prepared independently.
        """
        self.prepare_root()

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

    def write_pipeline(self, db_api: Any, pipeline: CTEPipeline) -> SplinkDataFrame:
        self.validate_db_api(db_api)
        return db_api._sql_pipeline_to_parquet(
            pipeline,
            output_path=Path(self.path),
            compression=self.compression,
            row_group_size=self.row_group_size,
            file_size_bytes=self.file_size_bytes,
        )

    def combine_chunks(self, db_api: Any) -> SplinkDataFrame:
        self.validate_db_api(db_api)
        return db_api._parquet_files_to_splink_dataframe(
            output_path=Path(self.path),
            parquet_glob=Path(self.path) / "*" / "*.parquet",
            templated_name="__splink__df_predict",
        )

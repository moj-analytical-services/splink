from __future__ import annotations

import os
import re
import shutil
import tempfile
from pathlib import Path

from .parquet_write_options import ParquetWriteOptions


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _writer_options_sql(options: ParquetWriteOptions) -> str:
    rendered = ["FORMAT PARQUET"]
    for field, keyword in (
        ("compression", "COMPRESSION"),
        ("compression_level", "COMPRESSION_LEVEL"),
        ("per_thread_output", "PER_THREAD_OUTPUT"),
        ("file_size_bytes", "FILE_SIZE_BYTES"),
        ("row_group_size", "ROW_GROUP_SIZE"),
    ):
        value = getattr(options, field)
        if value is None:
            continue
        if isinstance(value, bool):
            literal = "TRUE" if value else "FALSE"
        elif isinstance(value, str):
            literal = _quote(value)
        else:
            literal = str(value)
        rendered.append(f"{keyword} {literal}")
    return ", ".join(rendered)


class _ParquetMaterialiser:
    """Own backing files and generate SQL; never execute queries."""

    def __init__(
        self, directory: str | os.PathLike[str], write_options: ParquetWriteOptions
    ):
        directory = os.fspath(directory)
        if not isinstance(directory, str):
            raise TypeError("materialisation_dir must be a string or PathLike[str]")
        if not directory.strip() or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", directory):
            raise ValueError("materialisation_dir must be a non-empty local path")
        self._directory = Path(directory).resolve()
        self._write_options = write_options
        self._workspace: Path | None = None
        self._owned_paths: dict[str, Path] = {}
        self._pending_queries: dict[str, str] = {}

    def prepare_sql(self, sql: str, physical_name: str) -> str:
        if physical_name in self._owned_paths:
            raise ValueError(f"Backing files still owned for {physical_name}")
        if self._workspace is None:
            self._directory.mkdir(parents=True, exist_ok=True)
            self._workspace = Path(
                tempfile.mkdtemp(prefix="splink-", dir=self._directory)
            )
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", physical_name)[:80]
        path = Path(tempfile.mkdtemp(prefix=f"{safe_name}-", dir=self._workspace))
        self._owned_paths[physical_name] = path
        self._pending_queries[physical_name] = sql
        options = self._write_options
        multiple = options.per_thread_output or options.file_size_bytes is not None
        destination = path if multiple else path / "data.parquet"
        return (
            f"COPY ({sql}) TO {_quote(str(destination))} "
            f"({_writer_options_sql(options)})"
        )

    def view_sql(self, physical_name: str) -> str:
        glob = _quote(str(self._owned_paths[physical_name] / "*.parquet"))
        return (
            f"CREATE VIEW {physical_name} AS "
            f"SELECT * FROM read_parquet({glob}, hive_partitioning = false)"
        )

    def complete(self, physical_name: str) -> None:
        del self._pending_queries[physical_name]

    def has_pending_write(self, physical_name: str) -> bool:
        return physical_name in self._pending_queries

    def delete_backing_files(self, physical_name: str) -> None:
        path = self._owned_paths.get(physical_name)
        if path is None:
            return
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            if path.exists():
                raise
        except OSError as exc:
            raise OSError(
                f"Unable to remove Parquet backing files at {path}: {exc}"
            ) from exc
        del self._owned_paths[physical_name]
        self._pending_queries.pop(physical_name, None)

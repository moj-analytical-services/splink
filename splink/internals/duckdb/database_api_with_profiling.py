from __future__ import annotations

import re
from datetime import datetime, timezone
from os import PathLike
from pathlib import Path
from typing import Literal, Union

import duckdb

from .database_api import DuckDBAPI


class DuckDBAPIWithProfiling(DuckDBAPI):
    """DuckDB API that writes a profile for each table-creating query."""

    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str | None = None,
        query_profiling_dir: str | PathLike[str] = "tmp_query_profiling",
        profiling_format: Literal["text", "json"] = "text",
    ):
        if profiling_format not in ("text", "json"):
            raise ValueError("profiling_format must be either 'text' or 'json'")

        self._profiling_active = False
        super().__init__(connection=connection, output_schema=output_schema)
        self.query_profiling_dir = Path(query_profiling_dir)
        self.query_profiling_dir.mkdir(parents=True, exist_ok=True)
        self.profiling_format = profiling_format
        self._query_profile_counter = 0

    def _should_profile_sql(self, sql: str) -> bool:
        stripped_sql = sql.lstrip().upper()
        return stripped_sql.startswith("SELECT") or stripped_sql.startswith("WITH")

    def _next_query_profile_path(self, templated_name: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", templated_name).strip("_")
        if not safe_name:
            safe_name = "query"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        extension = "json" if self.profiling_format == "json" else "txt"
        while True:
            self._query_profile_counter += 1
            filename = (
                f"{timestamp}_{self._query_profile_counter:04d}_"
                f"{safe_name}_duckdb.{extension}"
            )
            profile_path = self.query_profiling_dir / filename
            if not profile_path.exists():
                return profile_path

    def _disable_profiling(self) -> None:
        self._profiling_active = False
        super()._execute_sql_against_backend("PRAGMA disable_profiling")

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        final_sql = super()._setup_for_execute_sql(sql, physical_name)
        if self._should_profile_sql(sql):
            profile_path = self._next_query_profile_path(physical_name)
            escaped_path = str(profile_path).replace("'", "''")
            duckdb_format = "json" if self.profiling_format == "json" else "query_tree"
            try:
                super()._execute_sql_against_backend(
                    f"PRAGMA enable_profiling='{duckdb_format}'"
                )
                super()._execute_sql_against_backend("PRAGMA profiling_mode='detailed'")
                super()._execute_sql_against_backend(
                    f"PRAGMA profiling_output='{escaped_path}'"
                )
            except BaseException:
                try:
                    self._disable_profiling()
                except Exception:
                    pass
                raise
            self._profiling_active = True

        return final_sql

    def _execute_sql_against_backend(self, final_sql: str) -> duckdb.DuckDBPyRelation:
        if not self._profiling_active:
            return super()._execute_sql_against_backend(final_sql)

        try:
            result = super()._execute_sql_against_backend(final_sql)
        except BaseException:
            try:
                self._disable_profiling()
            except Exception:
                pass
            raise
        else:
            self._disable_profiling()
            return result

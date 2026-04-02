from __future__ import annotations

import re
from os import PathLike
from pathlib import Path
from typing import Union

import duckdb

from .database_api import DuckDBAPI
from .dataframe import DuckDBDataFrame


class DuckDBAPIWithProfiling(DuckDBAPI):
    def __init__(
        self,
        connection: Union[str, duckdb.DuckDBPyConnection] = ":memory:",
        output_schema: str = None,
        query_profiling_dir: str | PathLike[str] = "tmp_query_profiling",
    ):
        super().__init__(connection=connection, output_schema=output_schema)
        self.query_profiling_dir = Path(query_profiling_dir)
        self.query_profiling_dir.mkdir(parents=True, exist_ok=True)
        self._query_profile_counter = 0
        self._pending_profile_path: Path | None = None
        self._pending_profile_sql: str | None = None

    def _should_profile_sql(self, sql: str) -> bool:
        stripped_sql = sql.lstrip().upper()
        return stripped_sql.startswith("SELECT") or stripped_sql.startswith("WITH")

    def _next_query_profile_path(self, templated_name: str) -> Path:
        self._query_profile_counter += 1
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", templated_name).strip("_")
        if not safe_name:
            safe_name = "query"
        filename = f"{self._query_profile_counter:04d}_{safe_name}_duckdb.txt"
        return self.query_profiling_dir / filename

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        if self._should_profile_sql(sql):
            self._pending_profile_path = self._next_query_profile_path(physical_name)
            self._pending_profile_sql = sql
        else:
            self._pending_profile_path = None
            self._pending_profile_sql = None

        return super()._setup_for_execute_sql(sql, physical_name)

    def _cleanup_for_execute_sql(
        self, table: duckdb.DuckDBPyRelation, templated_name: str, physical_name: str
    ) -> DuckDBDataFrame:
        try:
            output_df = self.table_to_splink_dataframe(templated_name, physical_name)
            if self._pending_profile_path is not None and self._pending_profile_sql:
                explain_result = super()._execute_sql_against_backend(
                    f"EXPLAIN ANALYZE {self._pending_profile_sql}"
                )
                rows = explain_result.fetchall()
                if len(rows) == 1 and len(rows[0]) == 2:
                    profile_text = rows[0][1]
                else:
                    profile_text = "\n".join(
                        " | ".join("" if value is None else str(value) for value in row)
                        for row in rows
                    )
                self._pending_profile_path.write_text(profile_text, encoding="utf-8")
            return output_df
        finally:
            self._pending_profile_path = None
            self._pending_profile_sql = None

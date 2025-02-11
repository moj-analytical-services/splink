from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from duckdb import DuckDBPyRelation
from pandas import DataFrame as pd_DataFrame

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from .database_api import DuckDBAPI


class DuckDBDataFrame(SplinkDataFrame):
    db_api: DuckDBAPI

    @property
    def columns(self) -> list[InputColumn]:
        sql = (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{self.physical_name}'"
        )
        col_strings = (self.db_api._execute_sql_against_backend(sql).to_df().to_dict())[
            "column_name"
        ].values()

        return [InputColumn(c, sqlglot_dialect_str="duckdb") for c in col_strings]

    def validate(self):
        pass

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        self.db_api.delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        duckdb_table = self.db_api._execute_sql_against_backend(sql)
        rows = duckdb_table.fetchall()
        column_names = [desc[0] for desc in duckdb_table.description]
        return [dict(zip(column_names, row)) for row in rows]

    def as_pandas_dataframe(self, limit: int = None) -> pd_DataFrame:
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api._execute_sql_against_backend(sql).to_df()

    def as_duckdbpyrelation(self, limit: int = None) -> DuckDBPyRelation:
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api._execute_sql_against_backend(sql)

    def to_parquet(self, filepath, overwrite=False):
        if not overwrite:
            self.check_file_exists(filepath)

        if not filepath.endswith(".parquet"):
            raise SyntaxError(
                f"The filepath you've entered to '{filepath}' is "
                "not a parquet file. Please ensure that the filepath "
                "ends with `.parquet` before retrying."
            )

        # create the directories recursively if they don't exist
        path = os.path.dirname(filepath)
        if path:
            os.makedirs(path, exist_ok=True)

        sql = f"COPY {self.physical_name} TO '{filepath}' (FORMAT PARQUET);"
        self.db_api._execute_sql_against_backend(sql)

    def to_csv(self, filepath, overwrite=False):
        if not overwrite:
            self.check_file_exists(filepath)

        if not filepath.endswith(".csv"):
            raise SyntaxError(
                f"The filepath you've entered '{filepath}' is "
                "not a csv file. Please ensure that the filepath "
                "ends with `.csv` before retrying."
            )

        # create the directories recursively if they don't exist
        path = os.path.dirname(filepath)
        if path:
            os.makedirs(path, exist_ok=True)

        sql = f"COPY {self.physical_name} TO '{filepath}' (HEADER, DELIMITER ',');"
        self.db_api._execute_sql_against_backend(sql)

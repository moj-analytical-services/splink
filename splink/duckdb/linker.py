from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import pandas as pd
from duckdb import DuckDBPyConnection

from ..input_column import InputColumn
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from ..database_api import DuckDBAPI


class DuckDBDataFrame(SplinkDataFrame):
    db_api: DuckDBAPI

    @property
    def columns(self) -> list[InputColumn]:
        d = self.as_record_dict(1)[0]

        col_strings = list(d.keys())
        return [InputColumn(c, sql_dialect="duckdb") for c in col_strings]

    def validate(self):
        pass

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        self.db_api._delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api._con.query(sql).to_df().to_dict(orient="records")

    def as_pandas_dataframe(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.db_api._con.query(sql).to_df()

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
        self.db_api._con.query(sql)

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
        self.db_api._con.query(sql)


class DuckDBLinker(Linker):
    """Manages the data linkage process and holds the data linkage model."""

    def __init__(
        self,
        input_table_or_tables: str | list,
        settings_dict: dict | str = None,
        connection: str | DuckDBPyConnection = ":memory:",
        set_up_basic_logging: bool = True,
        output_schema: str = None,
        input_table_aliases: str | list = None,
        validate_settings: bool = True,
    ):
        """The Linker object manages the data linkage process and holds the data linkage
        model.

        Most of Splink's functionality can  be accessed by calling functions (methods)
        on the linker, such as `linker.predict()`, `linker.profile_columns()` etc.

        Args:
            input_table_or_tables (Union[str, list]): Input data into the linkage model.
                Either a single string (the name of a table in a database) for
                deduplication jobs, or a list of strings  (the name of tables in a
                database) for link_only or link_and_dedupe
            settings_dict (dict | Path, optional): A Splink settings dictionary, or a
                 path toa json defining a settingss dictionary or pre-trained model.
                  If not provided when the object is created, can later be added using
                `linker.load_settings()` or `linker.load_model()` Defaults to None.
            connection (DuckDBPyConnection or str, optional):  Connection to duckdb.
                If a a string, will instantiate a new connection.  Defaults to :memory:.
                If the special :temporary: string is provided, an on-disk duckdb
                database will be created in a temporary directory.  This can be used
                if you are running out of memory using :memory:.
            set_up_basic_logging (bool, optional): If true, sets ups up basic logging
                so that Splink sends messages at INFO level to stdout. Defaults to True.
            output_schema (str, optional): Set the schema along which all tables
                will be created.
            input_table_aliases (Union[str, list], optional): Labels assigned to
                input tables in Splink outputs.  If the names of the tables in the
                input database are long or unspecific, this argument can be used
                to attach more easily readable/interpretable names. Defaults to None.
            validate_settings (bool, optional): When True, check your settings
                dictionary for any potential errors that may cause splink to fail.
        """

        # TODO: last bit of logic not translated
        input_tables = None
        input_aliases = None
        # Quickly check for casting error in duckdb/pandas
        for i, (table, alias) in enumerate(zip(input_tables, input_aliases)):
            if isinstance(table, pd.DataFrame):
                if isinstance(alias, pd.DataFrame):
                    alias = f"__splink__input_table_{i}"

                self._check_cast_error(alias)

    def _check_cast_error(self, alias):
        from duckdb import InvalidInputException

        error = InvalidInputException

        try:
            # fetch df is required as otherwise lazily evaluated and it breaks
            # other queries.
            self._con.execute(f"select * from {alias} limit 1").fetch_df()
        except error as e:
            raise InvalidInputException(
                "DuckDB cannot infer datatypes of one or more "
                "columns. Try converting dataframes "
                "to pyarrow tables before adding to your linker."
            ) from e

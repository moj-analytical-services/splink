from __future__ import annotations

import logging
import os
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd
from duckdb import DuckDBPyConnection

from ..input_column import InputColumn
from ..linker import Linker
from ..misc import (
    ensure_is_list,
)
from ..splink_dataframe import SplinkDataFrame
from .duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    duckdb_load_from_file,
    validate_duckdb_connection,
)

logger = logging.getLogger(__name__)


class DuckDBDataFrame(SplinkDataFrame):
    linker: DuckDBLinker

    @property
    def columns(self) -> list[InputColumn]:
        d = self.as_record_dict(1)[0]

        col_strings = list(d.keys())
        return [InputColumn(c, sql_dialect="duckdb") for c in col_strings]

    def validate(self):
        pass

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        self.linker._delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.linker._con.query(sql).to_df().to_dict(orient="records")

    def as_pandas_dataframe(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.linker._con.query(sql).to_df()

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
        self.linker._con.query(sql)

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
        self.linker._con.query(sql)


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

        self._sql_dialect_ = "duckdb"

        validate_duckdb_connection(connection, logger)

        if isinstance(connection, str):
            con_lower = connection.lower()
        if isinstance(connection, DuckDBPyConnection):
            con = connection
        elif con_lower == ":memory:":
            con = duckdb.connect(database=connection)
        elif con_lower == ":temporary:":
            con = create_temporary_duckdb_connection(self)
        else:
            con = duckdb.connect(database=connection)

        self._con = con

        # If user has provided pandas dataframes, need to register
        # them with the database, using user-provided aliases
        # if provided or a created alias if not
        input_tables = ensure_is_list(input_table_or_tables)
        input_tables = [
            duckdb_load_from_file(t) if isinstance(t, str) else t for t in input_tables
        ]

        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        accepted_df_dtypes = [pd.DataFrame]
        try:
            # If pyarrow is installed, add to the accepted list
            import pyarrow as pa

            accepted_df_dtypes.append(pa.lib.Table)
        except ImportError:
            pass

        super().__init__(
            input_tables,
            settings_dict,
            accepted_df_dtypes,
            set_up_basic_logging,
            input_table_aliases=input_aliases,
            validate_settings=validate_settings,
        )

        # Quickly check for casting error in duckdb/pandas
        for i, (table, alias) in enumerate(zip(input_tables, input_aliases)):
            if isinstance(table, pd.DataFrame):
                if isinstance(alias, pd.DataFrame):
                    alias = f"__splink__input_table_{i}"

                self._check_cast_error(alias)

        if output_schema:
            self._con.execute(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

    def _table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> DuckDBDataFrame:
        return DuckDBDataFrame(templated_name, physical_name, self)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):
        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        sql = f"""
        CREATE TABLE {physical_name}
        AS
        ({sql})
        """
        self._log_and_run_sql_execution(sql, templated_name, physical_name)

        return DuckDBDataFrame(templated_name, physical_name, self)

    def _run_sql_execution(self, final_sql, templated_name, physical_name):
        self._con.execute(final_sql)

    def register_table(self, input, table_name, overwrite=False):
        # If the user has provided a table name, return it as a SplinkDataframe
        if isinstance(input, str):
            return self._table_to_splink_dataframe(table_name, input)

        # Check if table name is already in use
        exists = self._table_exists_in_database(table_name)
        if exists:
            if not overwrite:
                raise ValueError(
                    f"Table '{table_name}' already exists in database. "
                    "Please use the 'overwrite' argument if you wish to overwrite"
                )
            else:
                self._con.unregister(table_name)

        self._table_registration(input, table_name)
        return self._table_to_splink_dataframe(table_name, table_name)

    def _table_registration(self, input, table_name):
        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Registration errors will automatically
        # occur if an invalid data type is passed as an argument
        self._con.register(table_name, input)

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        if seed:
            return f"USING SAMPLE bernoulli({percent}%) REPEATABLE({seed})"
        else:
            return f"USING SAMPLE {percent}% (bernoulli)"

    @property
    def _infinity_expression(self):
        return "cast('infinity' as float8)"

    def _table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        # From duckdb 0.5.0, duckdb will raise a CatalogException
        # which does not exist in 0.4.0 or before

        try:
            from duckdb import CatalogException

            error = (RuntimeError, CatalogException)
        except ImportError:
            error = RuntimeError

        try:
            self._con.execute(sql)
        except error:
            return False
        return True

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

    def _delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self._con.execute(drop_sql)

    def export_to_duckdb_file(self, output_path, delete_intermediate_tables=False):
        """
        https://stackoverflow.com/questions/66027598/how-to-vacuum-reduce-file-size-on-duckdb
        """
        if delete_intermediate_tables:
            self._delete_tables_created_by_splink_from_db()
        with TemporaryDirectory() as tmpdir:
            self._con.execute(f"EXPORT DATABASE '{tmpdir}' (FORMAT PARQUET);")
            new_con = duckdb.connect(database=output_path)
            new_con.execute(f"IMPORT DATABASE '{tmpdir}';")
            new_con.close()

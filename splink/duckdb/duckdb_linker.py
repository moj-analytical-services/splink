import logging
from typing import Union, List
from tempfile import TemporaryDirectory

from duckdb import DuckDBPyConnection
import duckdb
import pandas as pd

from .duckdb_helpers import (
    validate_duckdb_connection,
    create_temporary_duckdb_connection,
    duckdb_load_from_file,
)
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..logging_messages import execute_sql_logging_message_info, log_sql
from ..misc import (
    ensure_is_list,
    all_letter_combos,
)
from ..input_column import InputColumn

logger = logging.getLogger(__name__)


class DuckDBLinkerDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, duckdb_linker):
        super().__init__(templated_name, physical_name)
        self.duckdb_linker = duckdb_linker

    @property
    def columns(self) -> List[InputColumn]:
        d = self.as_record_dict(1)[0]

        col_strings = list(d.keys())
        return [InputColumn(c, sql_dialect="duckdb") for c in col_strings]

    def validate(self):
        pass

    def drop_table_from_database(self, force_non_splink_table=False):

        self._check_drop_table_created_by_splink(force_non_splink_table)

        self.duckdb_linker._delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):

        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.duckdb_linker._con.query(sql).to_df().to_dict(orient="records")

    def as_pandas_dataframe(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.duckdb_linker._con.query(sql).to_df()


class DuckDBLinker(Linker):
    """Manages the data linkage process and holds the data linkage model."""

    def __init__(
        self,
        input_table_or_tables: Union[str, list],
        settings_dict: dict = None,
        connection: Union[str, DuckDBPyConnection] = ":memory:",
        set_up_basic_logging: bool = True,
        output_schema: str = None,
        input_table_aliases: Union[str, list] = None,
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
            settings_dict (dict, optional): A Splink settings dictionary. If not
                provided when the object is created, can later be added using
                `linker.initialise_settings()` Defaults to None.
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

        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        # 'homogenised' means all entries are strings representing tables
        homogenised_tables = []
        homogenised_aliases = []

        default_aliases = all_letter_combos(len(input_tables))

        for (table, alias, default_alias) in zip(
            input_tables, input_aliases, default_aliases
        ):

            if type(alias).__name__ in ["DataFrame", "Table"]:
                alias = f"_{default_alias}"

            if type(table).__name__ in ["DataFrame", "Table"]:
                con.register(alias, table)
                table = alias

            homogenised_tables.append(duckdb_load_from_file(table))
            homogenised_aliases.append(alias)

        super().__init__(
            homogenised_tables,
            settings_dict,
            set_up_basic_logging,
            input_table_aliases=homogenised_aliases,
        )

        if output_schema:
            self._con.execute(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

    def _table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> DuckDBLinkerDataFrame:
        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):

        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        logger.debug(
            execute_sql_logging_message_info(
                templated_name, self._prepend_schema_to_table_name(physical_name)
            )
        )
        logger.log(5, log_sql(sql))

        sql = f"""
        CREATE TABLE {physical_name}
        AS
        ({sql})
        """
        self._con.execute(sql).fetch_df()

        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def register_table(self, input, table_name, overwrite=False):

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

        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Registration errors will automatically
        # occur if an invalid data type is passed as an argument
        self._con.register(table_name, input)
        return self._table_to_splink_dataframe(table_name, table_name)

    def initialise_settings(self, settings_dict: dict):
        if "sql_dialect" not in settings_dict:
            settings_dict["sql_dialect"] = "duckdb"
        super().initialise_settings(settings_dict)

    def _random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f"USING SAMPLE {percent}% (bernoulli)"

    @property
    def _infinity_expression(self):
        return "cast('infinity' as double)"

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

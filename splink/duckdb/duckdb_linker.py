import logging
import os
import tempfile
from typing import Union
import uuid
import sqlglot
from tempfile import TemporaryDirectory


import duckdb
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..logging_messages import execute_sql_logging_message_info, log_sql

logger = logging.getLogger(__name__)


class DuckDBLinkerDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, duckdb_linker):
        super().__init__(templated_name, physical_name)
        self.duckdb_linker = duckdb_linker

    @property
    def columns(self):
        d = self.as_record_dict(1)[0]

        return list(d.keys())

    def validate(self):
        pass

    def drop_table_from_database(self, force_non_splink_table=False):

        self._check_drop_table_created_by_splink(force_non_splink_table)

        self.duckdb_linker.delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):

        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.duckdb_linker.con.query(sql).to_df().to_dict(orient="records")

    def as_pandas_dataframe(self, limit=None):
        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.duckdb_linker.con.query(sql).to_df()


class DuckDBLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        connection=":memory:",
        set_up_basic_logging=True,
        output_schema=None,
        input_table_aliases: Union[str, list] = None,
    ):

        if connection == ":memory:":
            con = duckdb.connect(database=connection)
        else:
            if connection == ":temporary:":
                self.temp_dir = tempfile.TemporaryDirectory(dir=".")
                fname = uuid.uuid4().hex[:7]
                path = os.path.join(self.temp_dir.name, f"{fname}.duckdb")
                con = duckdb.connect(database=path, read_only=False)
            else:
                con = duckdb.connect(database=connection)

        self.con = con

        # If user has provided pandas dataframes, need to register
        # them with the database, using user-provided aliases
        # if provided or a created alias if not

        input_tables = self._ensure_is_list(input_table_or_tables)

        input_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        homogenised_tables = []
        homogenised_aliases = []

        for i, (table, alias) in enumerate(zip(input_tables, input_aliases)):

            if type(alias).__name__ == "DataFrame":
                alias = f"__splink__input_table_{i}"

            if type(table).__name__ == "DataFrame":
                con.register(alias, table)
                table = alias

            homogenised_tables.append(table)
            homogenised_aliases.append(alias)

        super().__init__(
            homogenised_tables,
            settings_dict,
            set_up_basic_logging,
            input_table_aliases=homogenised_aliases,
        )

        if output_schema:
            self.con.execute(
                f"""
                    CREATE SCHEMA IF NOT EXISTS {output_schema};
                    SET schema '{output_schema}';
                """
            )

    def _df_as_obj(self, templated_name, physical_name):
        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self.delete_table_from_database(physical_name)

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[0]

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
        self.con.execute(sql).fetch_df()

        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f"USING SAMPLE {percent}% (bernoulli)"

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"
        try:
            self.con.execute(sql)
        except RuntimeError:
            return False
        return True

    def records_to_table(self, records, as_table_name):
        for r in records:
            r["source_dataset"] = as_table_name

        import pandas as pd

        df = pd.DataFrame(records)
        self.con.register(as_table_name, df)

    def delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self.con.execute(drop_sql)

    def export_to_duckdb_file(self, output_path, delete_intermediate_tables=False):
        """
        https://stackoverflow.com/questions/66027598/how-to-vacuum-reduce-file-size-on-duckdb
        """
        if delete_intermediate_tables:
            self.delete_tables_created_by_splink_from_db()
        with TemporaryDirectory() as tmpdir:
            self.con.execute(f"EXPORT DATABASE '{tmpdir}' (FORMAT PARQUET);")
            new_con = duckdb.connect(database=output_path)
            new_con.execute(f"IMPORT DATABASE '{tmpdir}';")
            new_con.close()

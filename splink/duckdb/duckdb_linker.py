import logging
import os
import uuid

import sqlglot
from pandas import DataFrame as pd_DataFrame

import duckdb
from splink.linker import Linker, SplinkDataFrame

from splink.misc import create_temp_folder, create_db_folder

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

    def as_record_dict(self, limit=None):

        sql = f"select * from {self.physical_name}"
        if limit:
            sql += f" limit {limit}"

        return self.duckdb_linker.con.query(sql).to_df().to_dict(orient="records")

class DuckDBLinker(Linker):
    def __init__(self, settings_dict, input_tables, tf_tables={}, connection=":memory:"):

        if connection == ":temporary:":
            tmp_folder = create_temp_folder()
            con = duckdb.connect(database=f"{tmp_folder.name}/{uuid.uuid4().hex}.duckdb", read_only=False)
        else:
            if connection != ":memory:":
                create_db_folder(filepath=connection, file_ext=".duckdb")

            con = duckdb.connect(database=connection)

        self.con = con

        self.log_input_tables(con, input_tables, tf_tables)

        super().__init__(settings_dict, input_tables, tf_tables)

    def log_input_tables(self, con, input_tables, tf_tables):
        for templated_name, df in input_tables.items():
            # Make a table with this name
            con.register(templated_name, df)
            input_tables[templated_name] = templated_name

        for templated_name, df in tf_tables.items():
            # Make a table with this name
            templated_name = "__splink__df_" + templated_name
            con.register(templated_name, df)

    def _df_as_obj(self, templated_name, physical_name):
        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):
        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[0]

        sql = f"""
        CREATE TABLE IF NOT EXISTS {physical_name}
        AS
        ({sql})
        """
        output = self.con.execute(sql).fetch_df()

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

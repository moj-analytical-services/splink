import logging
import os
import tempfile
import uuid
import sqlglot
from tempfile import TemporaryDirectory


import duckdb
from splink.linker import Linker, SplinkDataFrame


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
    def __init__(self, settings_dict=None, input_tables={}, connection=":memory:"):

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

        # If inputted tables are pandas dataframes, register them against the db
        input_tables_new = {}

        for templated_name, df in input_tables.items():
            if type(df).__name__ == "DataFrame":
                db_tablename = f"__splink__{templated_name}"
                con.register(db_tablename, df)
                input_tables_new[templated_name] = db_tablename
            else:
                input_tables_new[templated_name] = templated_name
        input_tables = input_tables_new

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, templated_name, physical_name):
        return DuckDBLinkerDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        drop_sql = f"""
        DROP TABLE IF EXISTS {physical_name}"""
        self.con.execute(drop_sql)

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[0]

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

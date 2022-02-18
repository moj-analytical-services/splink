import logging
import hashlib
import os
import shutil
from itertools import chain

import sqlglot
from pandas import DataFrame as pd_DataFrame

import duckdb
from splink.linker import Linker, SplinkDataFrame

logger = logging.getLogger(__name__)


class DuckDBInMemoryLinkerDataFrame(SplinkDataFrame):
    def __init__(self, df_name, df_value, duckdb_linker):
        super().__init__(df_name, df_value)
        self.duckdb_linker = duckdb_linker

    @property
    def columns(self):
        return list(self.df_value.columns)

    def validate(self):
        if not type(self.df_value) is pd_DataFrame:
            raise ValueError(
                f"{self.df_name} is not a pandas dataframe.\n"
                "DuckDB In Memory Linker requires input data"
                " to be pandas dataframes",
            )

    def as_record_dict(self):
        return self.df_value.to_dict(orient="records")


class DuckDBInMemoryLinker(Linker):
    def __init__(self, settings_dict, input_tables, tf_tables={}):

        # only in here for initial testing so we can easily access/see files
        # (replace it with something that specifies our temp file storage)
        self.tmp_filepath = 'tmp_db'
        if not os.path.exists(self.tmp_filepath):
            os.mkdir(self.tmp_filepath)
        else:
            shutil.rmtree(self.tmp_filepath)
            os.mkdir(self.tmp_filepath)

        # create an in memory connection
        self.con = duckdb.connect(database=":memory:")
        self.register_input_tables(input_tables)

        super().__init__(settings_dict, input_tables, tf_tables)

    def _df_as_obj(self, df_name, df_value):
        return DuckDBInMemoryLinkerDataFrame(df_name, df_value, self)

    def register_input_tables(self, input_tables):
        [self.con.register(k, v) for k, v in input_tables.items()]

    def _duck_write_to_parquet(self, output_table_name, output_filename):
        self.con.execute(f"""COPY (SELECT * FROM '{output_table_name}')
        TO '{output_filename}.parquet' (FORMAT 'parquet')""")

    def _duck_write_to_csv(self, output_table_name, output_filename):
        self.con.execute(f"""COPY (SELECT * FROM '{output_table_name}')
        TO '{output_filename}.csv' (FORMAT 'csv')""")

    def generate_sql(self, sql, sql_pipeline: dict, output_table_name=None, transpile=True):
        # pipeline format: {sql_pipe: str, prev_dfs: list}

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[0]

        sql_pipeline["sql_pipe"] = ":".join(filter(lambda x: len(x) > 0,
                                            [sql_pipeline["sql_pipe"], sql]))
        sql_pipeline["prev_dfs"].append(output_table_name)

        # clean this up when we get the time...
        if output_table_name in self.cache_queries:
            sql_hash = hashlib.sha256(sql_pipeline["sql_pipe"].encode()).hexdigest()
            if sql_hash in self.sql_tracker:
                return {"sql_pipe": f"SELECT * FROM '{sql_hash}'", "prev_dfs": [output_table_name]}  # update our pipeline dict
            else:
                self.sql_tracker.setdefault(output_table_name,[]).append(sql_hash)  # log hash
                sql_to_run = self.combine_sql_queries(sql_pipeline)
                out = self.con.query(sql_to_run).to_df()  # execute and copy table instead? - might speed things up
                self.con.register(sql_hash, out)
                # self._duck_write_to_parquet(sql_hash, f"{self.tmp_filepath}/{sql_hash}")  # export to parquet
                sql_pipeline = {"sql_pipe": f"SELECT * FROM '{sql_hash}'", "prev_dfs": [output_table_name]}  # update our pipeline dict


        # print("----")
        # print(output_table_name)
        # print(sql)

        return(sql_pipeline)

    def new_execute_sql(self, sql_pipeline):
        """
        Temp method name while I move things around!
        """
        sql_to_run = self.combine_sql_queries(sql_pipeline)
        return self.con.execute(sql_to_run).fetch_df()

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f"USING SAMPLE {percent}% (bernoulli)"

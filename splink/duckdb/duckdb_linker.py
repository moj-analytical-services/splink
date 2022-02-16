import logging

import sqlglot
from pandas import DataFrame as pd_DataFrame

import duckdb
from splink3.linker import Linker, SplinkDataFrame

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

        con = duckdb.connect(database=":memory:")
        self.con = con

        super().__init__(settings_dict, input_tables, tf_tables)

    def _df_as_obj(self, df_name, df_value):
        return DuckDBInMemoryLinkerDataFrame(df_name, df_value, self)

    def execute_sql(self, sql, df_dict: dict, output_table_name=None, transpile=True):
        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="duckdb", pretty=True)[0]
        for df_obj in df_dict.values():
            table_name = df_obj.df_name
            df = df_obj.df_value
            self.con.register(table_name, df)

        # print("-------------")
        # print("-------------")

        output = self.con.query(sql).to_df()

        # from splink3.format_sql import format_sql

        # print(output_table_name)
        # try:
        #     display(output.head(8))
        # except:
        #     print(output.head(1).T)

        # print(sql)
        # print(f"as {output_table_name}")

        output_obj = self._df_as_obj(output_table_name, output)
        return {output_table_name: output_obj}

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        return f"USING SAMPLE {percent}% (bernoulli)"

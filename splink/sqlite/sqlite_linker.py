import sqlglot

from splink.linker import Linker, SplinkDataFrame
from math import pow, log2
from rapidfuzz.levenshtein import distance


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteDataFrame(SplinkDataFrame):
    def __init__(self, df_name, df_value, sqlite_linker):
        super().__init__(df_name, df_value)
        self.sqlite_linker = sqlite_linker

    @property
    def columns(self):
        sql = f"""
        PRAGMA table_info({self.df_value});
        """
        pragma_result = self.sqlite_linker.con.execute(sql).fetchall()
        cols = [r["name"] for r in pragma_result]
        return cols

    def validate(self):
        if not type(self.df_value) is str:
            raise ValueError(
                f"{self.df_name} is not a string dataframe.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of the "
                " sqlite table."
            )

        sql = f"""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='{self.df_value}';
        """

        res = self.sqlite_linker.con.execute(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.df_value} does not exist in the sqlite db provided.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of a "
                " sqlite table that exists in the provided db."
            )

    def as_record_dict(self):
        sql = f"""
        select *
        from {self.df_value};
        """
        cur = self.sqlite_linker.con.cursor()
        return cur.execute(sql).fetchall()


class SQLiteLinker(Linker):
    def __init__(self, settings_dict, input_tables, sqlite_connection):
        self.con = sqlite_connection
        self.con.row_factory = dict_factory
        self.con.create_function("log2", 1, log2)
        self.con.create_function("pow", 2, pow)
        self.con.create_function("levenshtein", 2, distance)

        self.con.create_function("greatest", 2, max)

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, df_name, df_value):
        return SQLiteDataFrame(df_name, df_value, self)

    def execute_sql(self, sql, df_dict: dict, output_table_name, transpile=True):

        drop_sql = f"""
        drop table if exists {output_table_name};
        """

        self.con.execute(drop_sql)

        for df in df_dict.values():
            sql = sql.replace(df.df_name, df.df_value)

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="sqlite")[0]

        sql = f"""
        create table {output_table_name}
        as
        {sql}
        """
        self.con.execute(sql)

        # import pandas as pd

        # df = pd.read_sql(f"select * from {output_table_name} limit 2", self.con)
        # from ..format_sql import format_sql

        # print(format_sql(sql))
        # print(output_table_name)
        # display(df)

        output_obj = self._df_as_obj(output_table_name, output_table_name)
        return {output_table_name: output_obj}

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""

        return (
            f"where id IN (SELECT id FROM table ORDER BY RANDOM() LIMIT {sample_size})"
        )

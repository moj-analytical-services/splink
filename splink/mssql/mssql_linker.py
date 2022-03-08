import pandas as pd
import sqlglot
from sqlalchemy.engine import reflection
from sqlalchemy import create_engine

from splink.linker import Linker, SplinkDataFrame
from splink.sql_functions import levenstein_sql

class MSSQLDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, mssql_linker, tsql=True):
        super().__init__(templated_name, physical_name)
        self.mssql_linker = mssql_linker


    @property
    def columns(self):
        sql = f"SELECT TOP 1 * FROM {self.physical_name}"
        return list(pd.read_sql(sql, self.mssql_linker).columns)  # requires sqlalchemy

    def validate(self):
        # add validation when we add in code to link directly to a db
        pass

    def as_record_dict(self, limit=None):
        sel = f"SELECT TOP {limit} *" if limit else "SELECT *"
        sql = f"""
        {sel} from {self.physical_name}
        """

        return pd.read_sql(sql, self.mssql_linker).to_dict(orient="records")  # requires sqlalchemy

def MSSQLConnection(
        server,
        database,
        username="",
        password="",
        driver="ODBC Driver 17 for SQL Server"
    ):

        driver = 'DRIVER={'+driver+'};SERVER='+f"{server};DATABASE={database};"
        if username:
            driver+=f"UID={username};"
        if password:
            driver+=f"PWD={password}"

        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % driver)
        engine.connect()
        return engine


class MSSQLLinker(Linker):
    def __init__(self, settings_dict, mssql_connection, input_tables, tf_tables={}):

        self.con = mssql_connection
        # add any sql functions we require
        self.add_sql_function("dbo.LEVENSHTEIN", levenstein_sql)

        self.log_input_tables(self.con, input_tables, tf_tables)
        super().__init__(settings_dict, input_tables, tsql=True)
        self.debug_mode=False  # set to true to force run each individual SQL statement

    def log_input_tables(self, con, input_tables, tf_tables):
        for templated_name, df in input_tables.items():
            # Make a table with this name
            df.to_sql(name=templated_name, con=self.con, index=False, if_exists='replace')  # requires sqlalchemy
            input_tables[templated_name] = templated_name

        for templated_name, df in tf_tables.items():
            # Make a table with this name
            templated_name = "__splink__df_" + templated_name
            df.to_sql(name=templated_name, con=self.con, index=False, if_exists='replace')  # requires sqlalchemy

    def _df_as_obj(self, templated_name, physical_name):
        return MSSQLDataFrame(templated_name, physical_name, self.con)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        # translation_dict = {
        #     "POW": "POWER",
        #     "LEVENSHTEIN": "dbo.LEVENSHTEIN",
        #     " = ": " LIKE " # might want to impose stricter settings for tsql
        # }
        translation_dict = {
            "POW": "POWER",
            "LEVENSHTEIN": "dbo.LEVENSHTEIN",
        }

        drop_sql = f"""
        DROP TABLE IF EXISTS {physical_name}"""
        self.run_sql(drop_sql)

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="sqlite")[0]

        # print(sql)
        print(f"===== {physical_name} =====")
        if self.debug_mode:
            sql = f"""
                SELECT *
                INTO {physical_name}
            FROM ({sql}) my_fancy_query
            """
        else:
            sep = "FROM" if transpile else "from"
            sql = f" INTO {physical_name} FROM ".join(
                sql.rsplit(sep, 1)
            )

        # translate anything not already converted from sqlite -> t-sql
        for k, v in translation_dict.items():
            sql = sql.replace(k, v)

        self.run_sql(sql)
        output_obj = self._df_as_obj(templated_name, physical_name)
        return output_obj

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""

        sample_size = int(sample_size)

        return f"SELECT TOP({sample_size}) * where unique_id IN (SELECT unique_id FROM __splink__df_concat_with_tf ORDER BY RANDOM())"

    def table_exists_in_database(self, table_name):
        tables = reflection.Inspector.from_engine(self.con).get_table_names()
        for t in tables:
            if t == table_name:
                return True
        return False

    def run_sql(self, sql):
        tmp_con = self.con.raw_connection()
        tmp_con.execute(sql)
        tmp_con.commit()
        tmp_con.close()

    def add_sql_function(self, function_name, function):
        drop_sql = f"DROP FUNCTION IF EXISTS {function_name}"
        self.run_sql(drop_sql)
        self.run_sql(function)

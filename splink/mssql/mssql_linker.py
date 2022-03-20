import pandas as pd
import sqlglot
from sqlalchemy.engine import reflection
from sqlalchemy import create_engine

from splink.linker import Linker, SplinkDataFrame
from splink.sql_functions import levenstein_sql, levenstein_sql_max
from splink.sql_transform.mssql_transform import _refactor_levenshtein, MSSQL

class MSSQLDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, mssql_linker, tsql=True):

        templated_name, physical_name = mssql_linker.create_schema_names(
            [templated_name, physical_name], mssql_syntax=False
        )
        super().__init__(templated_name, physical_name)
        self.mssql_linker = mssql_linker
        self.ms_physical_name = sqlglot.transpile(self.physical_name, read="spark", write="mssql")[0]


    @property
    def columns(self):
        sql = f'SELECT TOP 1 * FROM {self.ms_physical_name}'
        return list(pd.read_sql(sql, self.mssql_linker.con).columns)  # requires sqlalchemy

    def validate(self):
        # add validation when we add in code to link directly to a db
        pass

    def as_record_dict(self, limit=None):
        sel = f"SELECT TOP {limit} *" if limit else "SELECT *"
        sql = f"""
        {sel} from {self.ms_physical_name}
        """

        return pd.read_sql(sql, self.mssql_linker.con).to_dict(orient="records")  # requires sqlalchemy

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
    def __init__(
        self, settings_dict,
        mssql_connection, input_tables,
        schema="splink", tf_tables={}
        ):

        self.con = mssql_connection
        self.schema = schema if schema else None
        self.create_schema()

        # add any sql functions we require
        # self.add_sql_function("dbo.LEVENSHTEIN", levenstein_sql)
        self.add_sql_function("dbo.LEVENSHTEIN", levenstein_sql_max)

        if input_tables:
            self.log_input_tables(self.con, input_tables, tf_tables)

        super().__init__(settings_dict, input_tables, tsql=True)
        self.debug_mode=False  # set to true to force run each individual SQL statement

    def create_schema_names(self, names_list, mssql_syntax=False):
        if self.schema:
            if mssql_syntax:
                return [f'"{self.schema}"."{name}"' for name in names_list]
            else:
                return [f'`{self.schema}`.`{name}`' for name in names_list]
        return names_list

    def log_input_tables(self, con, input_tables, tf_tables):

        for templated_name, df in input_tables.items():
            # templated_name = self.create_schema_names([templated_name])
            # Make a table with this name
            df.to_sql(
                name=templated_name, con=self.con,
                index=False, if_exists='replace',
                schema = self.schema
            )  # requires sqlalchemy
            input_tables[templated_name] = templated_name

        for templated_name, df in tf_tables.items():
            # Make a table with this name
            templated_name = "__splink__df_" + templated_name
            df.to_sql(
                name=templated_name, con=self.con,
                index=False, if_exists='replace',
                schema = self.schema
            )  # requires sqlalchemy

    def _df_as_obj(self, templated_name, physical_name):
        return MSSQLDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        templated_name, physical_name = self.create_schema_names(
            [templated_name, physical_name], mssql_syntax=True
        )

        drop_sql = f"DROP TABLE IF EXISTS {physical_name}"
        self.run_sql([drop_sql])

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="mssql")[0]


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

        # print(sql)

        self.run_sql([sql])
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

    def run_sql(self, sql_list: list):
        tmp_con = self.con.raw_connection()
        for sql in sql_list:
            tmp_con.execute(sql)
        tmp_con.commit()
        tmp_con.close()

    def create_schema(self):
        if self.schema:
            schema_sql = f'CREATE SCHEMA {self.schema}'
            if self.schema not in self.con.dialect.get_schema_names(self.con):
                self.run_sql([schema_sql])

    def add_sql_function(self, function_name, function):
        # rm drop function when code is finalised
        drop_sql = f"DROP FUNCTION IF EXISTS {function_name}"
        self.run_sql([drop_sql, function])

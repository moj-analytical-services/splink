import sqlglot

from splink.linker import Linker, SplinkDataFrame
from math import pow, log2
from rapidfuzz.distance.Levenshtein import distance


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteDataFrame(SplinkDataFrame):
    def __init__(self, templated_name, physical_name, sqlite_linker):
        super().__init__(templated_name, physical_name)
        self.sqlite_linker = sqlite_linker

    @property
    def columns(self):
        sql = f"""
        PRAGMA table_info({self.physical_name});
        """
        pragma_result = self.sqlite_linker.con.execute(sql).fetchall()
        cols = [r["name"] for r in pragma_result]
        return cols

    def validate(self):
        if not type(self.physical_name) is str:
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
        AND name='{self.physical_name}';
        """

        res = self.sqlite_linker.con.execute(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the sqlite db provided.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of a "
                " sqlite table that exists in the provided db."
            )

    def as_record_dict(self):
        sql = f"""
        select *
        from {self.physical_name};
        """
        cur = self.sqlite_linker.con.cursor()
        return cur.execute(sql).fetchall()


class SQLiteLinker(Linker):
    def __init__(self, settings_dict=None, input_tables={}, connection=":memory:"):
        self.con = connection
        self.con.row_factory = dict_factory
        self.con.create_function("log2", 1, log2)
        self.con.create_function("pow", 2, pow)
        self.con.create_function("levenshtein", 2, distance)

        self.con.create_function("greatest", 2, max)

        super().__init__(settings_dict, input_tables)

    def _df_as_obj(self, templated_name, physical_name):
        return SQLiteDataFrame(templated_name, physical_name, self)

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):

        if transpile:
            sql = sqlglot.transpile(sql, read="spark", write="sqlite")[0]

        sql = f"""
        create table {physical_name}
        as
        {sql}
        """
        self.con.execute(sql)

        output_obj = self._df_as_obj(templated_name, physical_name)
        return output_obj

    def random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""

        sample_size = int(sample_size)

        return (
            "where unique_id IN (SELECT unique_id FROM __splink__df_concat_with_tf"
            f" ORDER BY RANDOM() LIMIT {sample_size})"
        )

    def table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        rec = self.con.execute(sql).fetchone()
        if not rec:
            return False
        else:
            return True

    def delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self.con.execute(drop_sql)

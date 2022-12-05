from typing import Union, List
import logging
from math import pow, log2
import pandas as pd

from ..logging_messages import execute_sql_logging_message_info, log_sql
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..input_column import InputColumn

logger = logging.getLogger(__name__)


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
    def columns(self) -> List[InputColumn]:
        sql = f"""
        PRAGMA table_info({self.physical_name});
        """
        pragma_result = self.sqlite_linker.con.execute(sql).fetchall()
        cols = [r["name"] for r in pragma_result]

        return [InputColumn(c, sql_dialect="sqlite") for c in cols]

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

    def drop_table_from_database(self, force_non_splink_table=False):

        self._check_drop_table_created_by_splink(force_non_splink_table)

        drop_sql = f"""
        DROP TABLE IF EXISTS {self.physical_name}"""
        cur = self.sqlite_linker.con.cursor()
        cur.execute(drop_sql)

    def as_record_dict(self, limit=None):
        sql = f"""
        select *
        from {self.physical_name};
        """
        if limit:
            sql += f" limit {limit}"
        cur = self.sqlite_linker.con.cursor()
        return cur.execute(sql).fetchall()


class SQLiteLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        connection=":memory:",
        set_up_basic_logging=True,
        input_table_aliases: Union[str, list] = None,
    ):

        self._sql_dialect_ = "sqlite"

        self.con = connection
        self.con.row_factory = dict_factory
        self.con.create_function("log2", 1, log2)
        self.con.create_function("pow", 2, pow)

        super().__init__(
            input_table_or_tables,
            settings_dict,
            set_up_basic_logging,
            input_table_aliases=input_table_aliases,
        )

    def _table_to_splink_dataframe(self, templated_name, physical_name):
        return SQLiteDataFrame(templated_name, physical_name, self)

    def initialise_settings(self, settings_dict: dict):
        if "sql_dialect" not in settings_dict:
            settings_dict["sql_dialect"] = "sqlite"
        super().initialise_settings(settings_dict)

    def _execute_sql_against_backend(self, sql, templated_name, physical_name):

        # In the case of a table already existing in the database,
        # execute sql is only reached if the user has explicitly turned off the cache
        self._delete_table_from_database(physical_name)

        logger.debug(execute_sql_logging_message_info(templated_name, physical_name))
        logger.log(5, log_sql(sql))

        sql = f"""
        create table {physical_name}
        as
        {sql}
        """
        self.con.execute(sql)

        output_obj = self._table_to_splink_dataframe(templated_name, physical_name)
        return output_obj

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
                self._delete_table_from_database(table_name)

        if isinstance(input, dict):
            input = pd.DataFrame(input)
        elif isinstance(input, list):
            input = pd.DataFrame.from_records(input)

        # Will error if an invalid data type is passed
        input.to_sql(table_name, self.con, index=False)
        return self._table_to_splink_dataframe(table_name, table_name)

    def _random_sample_sql(self, proportion, sample_size):
        if proportion == 1.0:
            return ""

        sample_size = int(sample_size)

        return (
            "where unique_id IN (SELECT unique_id FROM __splink__df_concat_with_tf"
            f" ORDER BY RANDOM() LIMIT {sample_size})"
        )

    @property
    def _infinity_expression(self):
        return "'infinity'"

    def _table_exists_in_database(self, table_name):
        sql = f"PRAGMA table_info('{table_name}');"

        rec = self.con.execute(sql).fetchone()
        if not rec:
            return False
        else:
            return True

    def _delete_table_from_database(self, name):
        drop_sql = f"""
        DROP TABLE IF EXISTS {name}"""
        self.con.execute(drop_sql)

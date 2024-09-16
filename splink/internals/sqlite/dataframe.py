from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from .database_api import SQLiteAPI


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SQLiteDataFrame(SplinkDataFrame):
    db_api: SQLiteAPI

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        PRAGMA table_info({self.physical_name});
        """
        pragma_result = self.db_api._execute_sql_against_backend(sql).fetchall()
        cols = [r["name"] for r in pragma_result]

        return [InputColumn(c, sqlglot_dialect_str="sqlite") for c in cols]

    def validate(self):
        if not isinstance(self.physical_name, str):
            raise ValueError(
                f"{self.templated_name} is not a string dataframe.\n"
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

        res = self.db_api._execute_sql_against_backend(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the sqlite db provided.\n"
                "SQLite Linker requires input data"
                " to be a string containing the name of a "
                " sqlite table that exists in the provided db."
            )

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)

        drop_sql = f"""
        DROP TABLE IF EXISTS {self.physical_name}"""
        cur = self.db_api.con.cursor()
        cur.execute(drop_sql)

    def as_record_dict(self, limit=None):
        sql = f"""
        select *
        from {self.physical_name}
        """
        if limit:
            sql += f" limit {limit}"
        sql += ";"
        cur = self.db_api.con.cursor()
        return cur.execute(sql).fetchall()

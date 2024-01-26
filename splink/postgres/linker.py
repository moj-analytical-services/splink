from __future__ import annotations

import logging

from sqlalchemy.engine import Engine

from ..input_column import InputColumn
from ..linker import Linker
from ..splink_dataframe import SplinkDataFrame
from ..unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)


class PostgresDataFrame(SplinkDataFrame):
    linker: PostgresLinker

    def __init__(self, df_name, physical_name, linker):
        super().__init__(df_name, physical_name, linker)
        self._db_schema = linker._db_schema
        self.physical_name = f"{self.physical_name}"

    @property
    def columns(self) -> list[InputColumn]:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{self.physical_name}';
        """
        res = self.linker._run_sql_execution(sql).fetchall()
        cols = [r["column_name"] for r in res]

        return [InputColumn(c, sql_dialect="postgres") for c in cols]

    def validate(self):
        if type(self.physical_name) is not str:
            raise ValueError(
                f"{self.df_name} is not a string dataframe.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of the"
                " postgres table."
            )

        sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = '{self.physical_name}';
        """

        res = self.linker._run_sql_execution(sql).fetchall()
        if len(res) == 0:
            raise ValueError(
                f"{self.physical_name} does not exist in the postgres db provided.\n"
                "Postgres Linker requires input data"
                " to be a string containing the name of a"
                " postgres table that exists in the provided db."
            )

    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)
        self.linker._delete_table_from_database(self.physical_name)

    def as_record_dict(self, limit=None):
        sql = f"""
        SELECT *
        FROM {self.physical_name}
        """
        if limit:
            sql += f" LIMIT {limit}"
        sql += ";"
        res = self.linker._run_sql_execution(sql).mappings().all()
        return [dict(r) for r in res]


class PostgresLinker(Linker):
    def __init__(
        self,
        input_table_or_tables,
        settings_dict=None,
        engine: Engine = None,
        set_up_basic_logging=True,
        input_table_aliases: str | list = None,
        validate_settings: bool = True,
        schema="splink",
        other_schemas_to_search: str | list = [],
    ):
        pass

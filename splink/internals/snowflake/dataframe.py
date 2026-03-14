from typing import TYPE_CHECKING

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from .database_api import SnowflakeAPI


class SnowflakeDataframe(SplinkDataFrame):
    db_api: "SnowflakeAPI"

    @property
    def columns(self) -> list[InputColumn]:
        sql = (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{self.physical_name}'"
        )
        res = self.db_api._execute_sql_against_backend(sql)
        cols = [col[0] for col in res.fetchall()]
        return [InputColumn(col, sqlglot_dialect_str="snowflake") for col in cols]

    def validate(self):
        """
        Concrete impl of super class; unclear what is expected.

        Pass for now
        """
        pass

    def _create_or_replace_temp_view(self, name: str, physical: str) -> None:
        self.db_api._execute_sql_against_backend(f"DROP VIEW IF EXISTS {name}")
        self.db_api._execute_sql_against_backend(
            f"CREATE TEMP VIEW {name} AS SELECT * FROM {physical}"
        )

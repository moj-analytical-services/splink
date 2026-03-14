from typing import TYPE_CHECKING, Any, Optional

from splink.internals.input_column import InputColumn
from splink.internals.splink_dataframe import SplinkDataFrame

if TYPE_CHECKING:
    from .database_api import SnowflakeAPI


class SnowflakeDataframe(SplinkDataFrame):
    db_api: "SnowflakeAPI"

    @property
    def columns(self) -> list[InputColumn]:
        sql = (
            f"SELECT lower(column_name) FROM information_schema.columns "
            f"WHERE table_name = '{self.physical_name.upper()}'"
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

    def as_record_dict(self, limit: Optional[int] = None) -> list[dict[str, Any]]:
        sql = f"SELECT * FROM {self.physical_name}"
        if limit:
            sql += f" LIMIT {limit}"

        snowflake_table = self.db_api._execute_sql_against_backend(sql)
        rows = snowflake_table.fetchall()
        column_names = [desc[0].lower() for desc in snowflake_table.description]
        return [dict(zip(column_names, row)) for row in rows]

    # Implemented as per other database backends - nothing special needed.
    def _drop_table_from_database(self, force_non_splink_table=False):
        self._check_drop_table_created_by_splink(force_non_splink_table)
        self.db_api.delete_table_from_database(self.physical_name)

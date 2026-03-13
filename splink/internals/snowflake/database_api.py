import logging
from typing import Any

from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import SnowflakeDialect

from .dataframe import SnowflakeDataframe

logger = logging.getLogger(__name__)


class SnowflakeAPI(DatabaseAPI[SnowflakeCursor[Any]]):
    sql_dialect = SnowflakeDialect()
    _con: SnowflakeConnection

    def __init__(self, connection: SnowflakeConnection, register_udfs: bool = True):
        super().__init__()

        self._con = connection

    def _execute_sql_against_backend(self, final_sql: str) -> SnowflakeCursor | None:
        return self._con.cursor().execute(final_sql)

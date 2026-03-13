import logging
from typing import Any

from snowflake.connector.cursor import SnowflakeCursor

from splink.internals.database_api import DatabaseAPI
from splink.internals.dialects import SnowflakeDialect

from .dataframe import SnowflakeDataframe

logger = logging.getLogger(__name__)


class SnowflakeAPI(DatabaseAPI[SnowflakeCursor[Any]]):
    sql_dialect = SnowflakeDialect()

import logging

import duckdb

from .dialects import DuckDBDialect
from .duckdb.duckdb_helpers.duckdb_helpers import (
    create_temporary_duckdb_connection,
    duckdb_load_from_file,
    validate_duckdb_connection,
)

logger = logging.getLogger(__name__)


class DatabaseAPI:
    pass


class DuckDBAPI(DatabaseAPI):
    sql_dialect = DuckDBDialect()
    def __init__(self, connection: str = ":memory:"):
        validate_duckdb_connection(connection, logger)

        if isinstance(connection, str):
            con_lower = connection.lower()
        if isinstance(connection, duckdb.DuckDBPyConnection):
            con = connection
        elif con_lower == ":memory:":
            con = duckdb.connect(database=connection)
        elif con_lower == ":temporary:":
            con = create_temporary_duckdb_connection(self)
        else:
            con = duckdb.connect(database=connection)

        self._con = con

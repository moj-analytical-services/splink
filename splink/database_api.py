from .dialects import DuckDBDialect


class DatabaseAPI:
    pass


class DuckDBAPI(DatabaseAPI):
    sql_dialect = DuckDBDialect

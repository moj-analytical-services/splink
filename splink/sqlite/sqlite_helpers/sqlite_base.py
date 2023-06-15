from ...dialect_base import (
    DialectBase,
)


class SqliteBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "sqlite"

    @property
    def _jaccard_name(self):
        raise AttributeError("Jaccard similarity not implemented for SQLite")

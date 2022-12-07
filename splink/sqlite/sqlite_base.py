from ..dialect_base import (
    DialectBase,
)


class SqliteBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "sqlite"

from ..dialect_base import DialectBase


class PostgresBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "postgres"

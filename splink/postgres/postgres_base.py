from ..dialect_base import DialectBase


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"

class PostgresBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "postgres"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

from ..dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"size(array_intersect({col_name_l}, {col_name_r}))"


class SparkBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "spark"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

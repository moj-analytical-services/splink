from ..dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"size(array_intersect({col_name_l}, {col_name_r}))"


def datediff_sql(col_name_l, col_name_r, date_threshold, date_metric):

    if date_metric == "day":
        date_f = f"abs(datediff({col_name_l}, {col_name_r}))"
    elif date_metric in ["month", "year"]:
        date_f = f"floor(abs(months_between({col_name_l}, {col_name_r})"
        if date_metric == "year":
            date_f += " / 12))"
        else:
            date_f += "))"

    return f"""
        {date_f} <= {date_threshold}
    """


class SparkBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "spark"

    @property
    def _datediff_function(self):
        return datediff_sql

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

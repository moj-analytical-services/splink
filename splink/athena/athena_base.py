from ..dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"


def datediff_sql(
    col_name_l,
    col_name_r,
    date_threshold,
    date_metric,
    cast_str=False,
    date_format=None,
):
    if date_format is None:
        date_format = "%Y-%m-%d"

    if cast_str:
        return f"""
            abs(date_diff('{date_metric}',
                DATE {col_name_l}, '{date_format}'),
                DATE({col_name_r}, '{date_format}'))
                ) <= {date_threshold}
        """
    else:
        return f"""
            abs(date_diff('{date_metric}',
                {col_name_l},
                {col_name_r})
                ) <= {date_threshold}
        """


class AthenaBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "presto"

    @property
    def _levenshtein_name(self):
        return "levenshtein_distance"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

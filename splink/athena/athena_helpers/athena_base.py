from ...dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"


def regex_extract_sql(col_name, regex):
    return f"""
        regexp_extract({col_name}, '{regex}')
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

    @property
    def _regex_extract_function(self):
        return regex_extract_sql

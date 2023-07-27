from ...dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"cardinality(array_intersect({col_name_l}, {col_name_r}))"


def regex_extract_sql(col_name, regex):
    return f"""
        regexp_extract({col_name}, '{regex}')
    """


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
            abs(DATE_DIFF('{date_metric}',
                DATE_PARSE({col_name_l}, '{date_format}'),
                DATE_PARSE({col_name_r}, '{date_format}'))
                ) <= {date_threshold}
        """
    else:
        return f"""
            abs(DATE_DIFF('{date_metric}',
                {col_name_l},
                {col_name_r})
                ) <= {date_threshold}
        """


def valid_date_sql(col_name, date_format=None):
    if date_format is None:
        date_format = "%Y-%m-%d"

    return f"""
        try(date_parse({col_name}, '{date_format}'))
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

    @property
    def _datediff_function(self):
        return datediff_sql

    @property
    def _valid_date_function(self):
        return valid_date_sql

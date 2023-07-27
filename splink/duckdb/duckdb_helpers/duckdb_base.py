from ...dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    # sum of individual (unique) array sizes, minus the (unique) union
    return (
        f"list_unique({col_name_l}) + list_unique({col_name_r})"
        f" - list_unique(list_concat({col_name_l}, {col_name_r}))"
    )


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
                strptime({col_name_l}, '{date_format}'),
                strptime({col_name_r}, '{date_format}'))
                ) <= {date_threshold}
        """
    else:
        return f"""
            abs(date_diff('{date_metric}', {col_name_l},
              {col_name_r})) <= {date_threshold}
        """


def valid_date_sql(col_name, date_format=None):
    if date_format is None:
        date_format = "%Y-%m-%d"

    return f"""
        try_strptime({col_name}, '{date_format}')
    """


def regex_extract_sql(col_name, regex):
    return f"""
        regexp_extract({col_name}, '{regex}')
    """


class DuckDBBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "duckdb"

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

    @property
    def _datediff_function(self):
        return datediff_sql

    @property
    def _valid_date_function(self):
        return valid_date_sql

    @property
    def _regex_extract_function(self):
        return regex_extract_sql

    @property
    def _jaro_name(self):
        return "jaro_similarity"

    @property
    def _jaro_winkler_name(self):
        return "jaro_winkler_similarity"

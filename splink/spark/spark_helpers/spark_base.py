from ...dialect_base import (
    DialectBase,
)


def size_array_intersect_sql(col_name_l, col_name_r):
    return f"size(array_intersect({col_name_l}, {col_name_r}))"


def datediff_sql(
    col_name_l,
    col_name_r,
    date_threshold,
    date_metric,
    cast_str=False,
    date_format=None,
):
    if date_format is None:
        date_format = "yyyy-MM-dd"

    if cast_str:
        col_name_l = f"to_timestamp({col_name_l}, '{date_format}')"
        col_name_r = f"to_timestamp({col_name_r}, '{date_format}')"
    if date_metric == "day":
        date_f = f"abs(datediff({col_name_l}, {col_name_r}))"
    elif date_metric in ["month", "year"]:
        date_f = f"ceil(abs(months_between({col_name_l}, {col_name_r})"
        if date_metric == "year":
            date_f += " / 12))"
        else:
            date_f += "))"

    return f"""
        {date_f} <= {date_threshold}
    """


def valid_date_sql(col_name, date_format=None):
    if date_format is None:
        date_format = "yyyy-MM-dd"

    return f"""
        to_date({col_name}, '{date_format}')
    """


def regex_extract_sql(col_name, regex):
    if "\\" in regex:
        raise SyntaxError(
            "Regular expressions containing “\\” (the python escape character) "
            "are not compatible with Splink’s Spark linker. "
            "Please consider using alternative syntax, "
            "for example replacing “\\d” with “[0-9]”."
        )
    else:
        return f"""
        regexp_extract({col_name}, '{regex}', 0)
    """


class SparkBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "spark"

    @property
    def _datediff_function(self):
        return datediff_sql

    @property
    def _valid_date_function(self):
        return valid_date_sql

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

    @property
    def _regex_extract_function(self):
        return regex_extract_sql

    @property
    def _jaro_name(self):
        return "jaro_sim"

    @property
    def _damerau_levenshtein_name(self):
        return "damerau_levenshtein"

from ...dialect_base import DialectBase


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
    if date_metric not in ("day", "month", "year"):
        raise ValueError("`date_metric` must be one of ('day', 'month', 'year')")

    if date_format is None:
        date_format = "yyyy-MM-dd"

    if cast_str:
        datediff_args = f"""
            to_date({col_name_l}, '{date_format}'),
            to_date({col_name_r}, '{date_format}')
        """
    else:
        datediff_args = f"{col_name_l}, {col_name_r}"

    if date_metric == "day":
        date_f = f"""
            abs(
                datediff(
                    {datediff_args}
                )
            )
        """
    elif date_metric in ["month", "year"]:
        date_f = f"""
            floor(abs(
                ave_months_between(
                    {datediff_args}
                )"""
        if date_metric == "year":
            date_f += " / 12))"
        else:
            date_f += "))"
    return f"""
        {date_f} <= {date_threshold}
    """


class PostgresBase(DialectBase):
    @property
    def _sql_dialect(self):
        return "postgres"

    @property
    def _datediff_function(self):
        return datediff_sql

    @property
    def _size_array_intersect_function(self):
        return size_array_intersect_sql

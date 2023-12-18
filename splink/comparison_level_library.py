from typing import List, Union

from sqlglot import TokenError, parse_one

from .column_expression import ColumnExpression

# import composition functions for export
from .comparison_level_composition import And, Not, Or  # NOQA: F401
from .comparison_level_creator import ComparisonLevelCreator
from .comparison_level_sql import great_circle_distance_km_sql
from .dialects import SplinkDialect


def unsupported_splink_dialects(unsupported_dialects: List[str]):
    def decorator(func):
        def wrapper(self, splink_dialect: SplinkDialect, *args, **kwargs):
            if splink_dialect.name in unsupported_dialects:
                raise ValueError(
                    f"Dialect {splink_dialect.name} is not supported "
                    f"for {self.__class__.__name__}"
                )
            return func(self, splink_dialect, *args, **kwargs)

        return wrapper

    return decorator


def _translate_sql_string(
    sqlglot_base_dialect_sql: str,
    to_sqlglot_dialect: str,
    from_sqlglot_dialect: str = None,
) -> str:
    tree = parse_one(sqlglot_base_dialect_sql, read=from_sqlglot_dialect)

    return tree.sql(dialect=to_sqlglot_dialect)


def validate_distance_threshold(
    lower_bound: Union[int, float],
    upper_bound: Union[int, float],
    distance_threshold: Union[int, float],
    level_name: str,
) -> Union[int, float]:
    """Check if a distance threshold falls between two bounds."""
    if lower_bound <= distance_threshold <= upper_bound:
        return distance_threshold
    else:
        raise ValueError(
            "'distance_threshold' must be between "
            f"{lower_bound} and {upper_bound} for {level_name}"
        )


class NullLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
    ):
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.is_null_level = True

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"{col.name_l} IS NULL OR {col.name_r} IS NULL"

    def create_label_for_charts(self) -> str:
        return f"{self.col_expression.label} is NULL"


class ElseLevel(ComparisonLevelCreator):
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return "ELSE"

    def create_label_for_charts(self) -> str:
        return "All other comparisons"


class CustomLevel(ComparisonLevelCreator):
    def __init__(
        self,
        sql_condition: str,
        label_for_charts: str = None,
        base_dialect_str: str = None,
    ):
        """Represents a comparison level with a custom sql expression

        Must be in a form suitable for use in a SQL CASE WHEN expression
        e.g. "substr(name_l, 1, 1) = substr(name_r, 1, 1)"

        Args:
            sql_condition (str): SQL condition to assess similarity
            label_for_charts (str, optional): A label for this level to be used in
                charts. Default None, so that `sql_condition` is used
            base_dialect_str (str, optional): If specified, the SQL dialect that
                this expression will parsed as when attempting to translate to
                other backends

        """
        self.sql_condition = sql_condition
        self.label_for_charts = label_for_charts
        self.base_dialect_str = base_dialect_str

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        sql_condition = self.sql_condition
        if self.base_dialect_str is not None:
            base_dialect = SplinkDialect.from_string(self.base_dialect_str)
            # if we are told it is one dialect, but try to create comparison level
            # of another, try to translate with sqlglot
            if sql_dialect != base_dialect:
                base_dialect_sqlglot_name = base_dialect.sqlglot_name

                # as default, translate condition into our dialect
                try:
                    sql_condition = _translate_sql_string(
                        sql_condition,
                        sql_dialect.sqlglot_name,
                        base_dialect_sqlglot_name,
                    )
                # if we hit a sqlglot error, assume users knows what they are doing,
                # e.g. it is something custom / unknown to sqlglot
                # error will just appear when they try to use it
                except TokenError:
                    pass
        return sql_condition

    def create_label_for_charts(self) -> str:
        return (
            self.label_for_charts
            if self.label_for_charts is not None
            else self.sql_condition
        )


class ExactMatchLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        term_frequency_adjustments: bool = False,
    ):
        """Represents a comparison level where there is an exact match

        e.g. val_l = val_r

        Args:
            col_name (str): Input column name
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.

        """
        config = {}

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)

        if term_frequency_adjustments:
            if not self.col_expression.is_pure_column_or_column_reference:
                raise ValueError(
                    "The boolean term_frequency_adjustments argument"
                    " can only be used if the column name has no "
                    "transforms applied to it such as lower(), "
                    "substr() etc."
                )

            config["tf_adjustment_column"] = col_name
            config["tf_adjustment_weight"] = 1.0
            # leave tf_minimum_u_value as None

        self.configure(**config)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"{col.name_l} = {col.name_r}"

    def create_label_for_charts(self) -> str:
        return f"Exact match on {self.col_expression.label}"


class ColumnsReversedLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name_1: Union[str, ColumnExpression],
        col_name_2: Union[str, ColumnExpression],
    ):
        """Represents a comparison level where the columns are reversed. For example,
        if surname is in the forename field and vice versa

        Args:
            col_name_1 (str): First column, e.g. forename
            col_name_2 (str): Second column, e.g. surname
        """
        self.col_expression_1 = ColumnExpression.instantiate_if_str(col_name_1)
        self.col_expression_2 = ColumnExpression.instantiate_if_str(col_name_2)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression_1.sql_dialect = sql_dialect
        self.col_expression_2.sql_dialect = sql_dialect
        col_1 = self.col_expression_1
        col_2 = self.col_expression_2

        return (
            f"{col_1.name_l} = {col_2.name_r} " f"AND {col_1.name_r} = {col_2.name_l}"
        )

    def create_label_for_charts(self) -> str:
        col_1 = self.col_expression_1
        col_2 = self.col_expression_2
        return f"Match on reversed cols: {col_1.label} and {col_2.label}"


class LevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: Union[str, ColumnExpression], distance_threshold: int):
        """A comparison level using a sqlglot_dialect_name distance function

        e.g. levenshtein(val_l, val_r) <= distance_threshold

        Args:
            col_name (str): Input column name
            distance_threshold (int): The threshold to use to assess
                similarity
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        lev_fn = sql_dialect.levenshtein_function_name
        return f"{lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Levenshtein distance of {col.label} <= {self.distance_threshold}"


class DamerauLevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: Union[str, ColumnExpression], distance_threshold: int):
        """A comparison level using a Damerau-Levenshtein distance function

        e.g. damerau_levenshtein(val_l, val_r) <= distance_threshold

        Args:
            col_name (str): Input column name
            distance_threshold (int): The threshold to use to assess
                similarity
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        dm_lev_fn = sql_dialect.damerau_levenshtein_function_name
        return f"{dm_lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self, sql_dialect: SplinkDialect) -> str:
        return (
            f"Damerau-Levenshtein distance of {self.col_expression.label} "
            f"<= {self.distance_threshold}"
        )


class JaroWinklerLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        distance_threshold: Union[int, float],
    ):
        """A comparison level using a Jaro-Winkler distance function

        e.g. `jaro_winkler(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        jw_fn = sql_dialect.jaro_winkler_function_name
        return f"{jw_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Jaro-Winkler distance of {col.label} >= {self.distance_threshold}"


class JaroLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        distance_threshold: Union[int, float],
    ):
        """A comparison level using a Jaro distance function

        e.g. `jaro(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        j_fn = sql_dialect.jaro_function_name
        return f"{j_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Jaro distance of '{col.label} >= {self.distance_threshold}'"


class JaccardLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        distance_threshold: Union[int, float],
    ):
        """A comparison level using a Jaccard distance function

        e.g. `jaccard(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        j_fn = sql_dialect.jaccard_function_name
        return f"{j_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Jaccard distance of '{col.label} >= {self.distance_threshold}'"


class DatediffLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        date_threshold: int,
        date_metric: str = "day",  ##TODO: Lock down to sqlglot supported values
    ):
        """A comparison level using a date difference function

        e.g. abs(date_diff('day', "mydate_l", "mydate_r")) <= 2  (duckdb dialect)

        Args:
            col_name (str): Input column name
            date_threshold (int): The threshold for the date difference
            date_metric (str): The unit of time ('day', 'month', 'year')
                for the threshold
            cast_strings_to_date (bool): Whether to cast string columns to date format
            date_format (str): The format of the date string
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.date_threshold = date_threshold
        self.date_metric = date_metric

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        """Use sqlglot to auto transpile where possible
        Where sqlglot auto transpilation doesn't work correctly, a date_diff function
        must be implemented in the dialect, which will be used instead
        """

        if self.date_metric not in ("day", "month", "year"):
            raise ValueError("`date_metric` must be one of ('day', 'month', 'year')")

        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression

        if hasattr(sql_dialect, "date_diff"):
            return sql_dialect.date_diff(self)

        # Use col as placeholder here because there's no guarantee the complex
        # transformed ColumnExpression will autotranspile
        sqlglot_base_dialect_sql = (
            f"ABS(DATE_DIFF(___col____l, "
            f"___col____r, '{self.date_metric}'))"
            f"<= {self.date_threshold}"
        )
        sqlglot_dialect_name = sql_dialect.sqlglot_name
        translated = _translate_sql_string(
            sqlglot_base_dialect_sql, sqlglot_dialect_name
        )
        col = self.col_expression
        translated = translated.replace("___col____l", col.name_l)
        translated = translated.replace("___col____r", col.name_r)
        return translated

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return (
            f"Date difference of '{col.label} <= "
            f"{self.date_threshold} {self.date_metric}'"
        )


class DistanceInKMLevel(ComparisonLevelCreator):
    def __init__(
        self,
        lat_col: str,
        long_col: str,
        km_threshold: Union[int, float],
        not_null: bool = False,
    ):
        """Use the haversine formula to transform comparisons of lat,lngs
        into distances measured in kilometers

        Arguments:
            lat_col (str): The name of a latitude column or the respective array
                or struct column column containing the information
                For example: long_lat['lat'] or long_lat[0]
            long_col (str): The name of a longitudinal column or the respective array
                or struct column column containing the information, plus an index.
                For example: long_lat['long'] or long_lat[1]
            km_threshold (int): The total distance in kilometers to evaluate your
                comparisons against
            not_null (bool): If true, ensure no attempt is made to compute this if
              any inputs are null. This is only necessary if you are not
                capturing nulls elsewhere in your comparison level.

        """
        self.lat_col_expression = ColumnExpression.instantiate_if_str(lat_col)
        self.long_col_expression = ColumnExpression.instantiate_if_str(long_col)

        self.km_threshold = km_threshold
        self.not_null = not_null

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        ColumnExpression.instantiate_if_str(self.lat_col, splink_dialect=sql_dialect)
        ColumnExpression.instantiate_if_str(self.long_col, splink_dialect=sql_dialect)

        self.lat_col_expression.sql_dialect = sql_dialect
        lat_col = self.lat_col_expression

        self.long_col_expression.sql_dialect = sql_dialect
        long_col = self.long_col_expression

        lat_l, lat_r = lat_col.name_l, lat_col.name_r
        long_l, long_r = long_col.name_l, long_col.name_r

        distance_km_sql = (
            f"{great_circle_distance_km_sql(lat_l, lat_r, long_l, long_r)} "
            f"<= {self.km_threshold}"
        )

        if self.not_null:
            null_sql = " AND ".join(
                [f"{c} is not null" for c in [lat_r, lat_l, long_l, long_r]]
            )
            distance_km_sql = f"({null_sql}) AND {distance_km_sql}"

        return distance_km_sql

    def create_label_for_charts(self) -> str:
        return f"Distance less than {self.km_threshold}km"


class ArrayIntersectLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, min_intersection: int):
        """Represents a comparison level based around the size of an intersection of
        arrays

        Args:
            col_name (str): Input column name
            min_intersection (int, optional): The minimum cardinality of the
                intersection of arrays for this comparison level. Defaults to 1
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.min_intersection = min_intersection

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        if hasattr(sql_dialect, "array_intersect"):
            return sql_dialect.array_intersect(self)

        sqlglot_dialect_name = sql_dialect.sqlglot_name

        sqlglot_base_dialect_sql = f"""
            ARRAY_SIZE(ARRAY_INTERSECT(___col____l, ___col____r))
                >= {self.min_intersection}
                """
        translated = _translate_sql_string(
            sqlglot_base_dialect_sql, sqlglot_dialect_name
        )

        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        col = self.col_expression
        translated = translated.replace("___col____l", col.name_l)
        translated = translated.replace("___col____r", col.name_r)
        return translated

    def create_label_for_charts(self) -> str:
        return f"Array intersection size >= {self.min_intersection}"


class PercentageDifferenceLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, percentage_threshold: float):
        """
        Represents a comparison level where the difference between two numerical
        values is within a specified percentage threshold.

        The percentage difference is calculated as the absolute difference between the
        two values divided by the greater of the two values.

        Args:
            col_name (str): Input column name.
            percentage_threshold (float): The threshold percentage to use
                to assess similarity e.g. 0.1 for 10%.
        """
        if not 0 <= percentage_threshold <= 1:
            raise ValueError("percentage_threshold must be between 0 and 1")

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.percentage_threshold = percentage_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return (
            f"(ABS({col.name_l} - {col.name_r}) / "
            f"(CASE "
            f"WHEN {col.name_r} > {col.name_l} THEN {col.name_r} "
            f"ELSE {col.name_l} "
            f"END)) < {self.percentage_threshold}"
        )

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return (
            f"Percentage difference of '{col.label}' "
            f"within {self.percentage_threshold:,.2%}"
        )

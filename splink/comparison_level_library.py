from typing import List, Union

from sqlglot import parse_one

from .comparison_level_creator import ComparisonLevelCreator
from .comparison_level_sql import great_circle_distance_km_sql
from .dialects import SplinkDialect
from .input_column import InputColumn


def input_column_factory(name, splink_dialect: SplinkDialect) -> InputColumn:
    return InputColumn(name, sql_dialect=splink_dialect.sqlglot_name)


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
    def __init__(self, col_name: str):
        self.col_name = col_name
        self.is_null_level = True

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        return f"{col.name_l} IS NULL OR {col.name_r} IS NULL"

    def create_label_for_charts(self) -> str:
        return f"{self.col_name} is NULL"


class ElseLevel(ComparisonLevelCreator):
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return "ELSE"

    def create_label_for_charts(self) -> str:
        return "All other comparisons"


class ExactMatchLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, term_frequency_adjustments: bool = False):
        """Represents a comparison level where there is an exact match

        e.g. val_l = val_r

        Args:
            col_name (str): Input column name
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.

        """
        config = {}
        if term_frequency_adjustments:
            config["tf_adjustment_column"] = col_name
            config["tf_adjustment_weight"] = 1.0
            # leave tf_minimum_u_value as None
        self.col_name = col_name
        self.configure(**config)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        return f"{col.name_l} = {col.name_r}"

    def create_label_for_charts(self) -> str:
        return f"Exact match on {self.col_name}"


class ColumnsReversedLevel(ComparisonLevelCreator):
    def __init__(self, col_name_1: str, col_name_2: str):
        """Represents a comparison level where the columns are reversed.  For example,
        if surname is in the forename field and vice versa

        Args:
            col_name_1 (str): First column, e.g. forename
            col_name_2 (str): Second column, e.g. surname
        """
        self.col_name_1 = col_name_1
        self.col_name_2 = col_name_2

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        input_col_1 = input_column_factory(self.col_name_1, splink_dialect=sql_dialect)
        input_col_2 = input_column_factory(self.col_name_2, splink_dialect=sql_dialect)

        return (
            f"{input_col_1.name_l} = {input_col_2.name_r} "
            f"AND {input_col_1.name_r} = {input_col_2.name_l}"
        )

    def create_label_for_charts(self) -> str:
        return f"Match on reversed cols: {self.col_name_1} and {self.col_name_2}"


class LevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: int):
        """A comparison level using a sqlglot_dialect_name distance function

        e.g. levenshtein(val_l, val_r) <= distance_threshold

        Args:
            col_name (str): Input column name
            distance_threshold (int): The threshold to use to assess
                similarity
        """
        self.col_name = col_name
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        lev_fn = sql_dialect.levenshtein_function_name
        return f"{lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return f"Levenshtein distance of {self.col_name} <= {self.distance_threshold}"


class DamerauLevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: int):
        """A comparison level using a Damerau-Levenshtein distance function

        e.g. damerau_levenshtein(val_l, val_r) <= distance_threshold

        Args:
            col_name (str): Input column name
            distance_threshold (int): The threshold to use to assess
                similarity
        """
        self.col_name = col_name
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        dm_lev_fn = sql_dialect.damerau_levenshtein_function_name
        return f"{dm_lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return (
            f"Damerau-Levenshtein distance of {self.col_name} "
            f"<= {self.distance_threshold}"
        )


class JaroWinklerLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: Union[int, float]):
        """A comparison level using a Jaro-Winkler distance function

        e.g. `jaro_winkler(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_name = col_name
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        jw_fn = sql_dialect.jaro_winkler_function_name
        return f"{jw_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return (
            f"Jaro-Winkler distance of '{self.col_name} >= {self.distance_threshold}'"
        )


class JaroLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: Union[int, float]):
        """A comparison level using a Jaro distance function

        e.g. `jaro(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_name = col_name
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        j_fn = sql_dialect.jaro_function_name
        return f"{j_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return f"Jaro distance of '{self.col_name} >= {self.distance_threshold}'"


class JaccardLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: Union[int, float]):
        """A comparison level using a Jaccard distance function

        e.g. `jaccard(val_l, val_r) >= distance_threshold`

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_name = col_name
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        j_fn = sql_dialect.jaccard_function_name
        return f"{j_fn}({col.name_l}, {col.name_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return f"Jaccard distance of '{self.col_name} >= {self.distance_threshold}'"


class DatediffLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: str,
        date_threshold: int,
        date_metric: str = "day",  ##TODO: Lock down to sqlglot supported values
        cast_strings_to_date: bool = False,
        date_format: str = "%Y-%m-%d",
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
        self.col_name = col_name
        self.date_threshold = date_threshold
        self.date_metric = date_metric
        self.cast_strings_to_date = cast_strings_to_date
        self.date_format = date_format

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        """Use sqlglot to auto transpile where possible
        Where sqlglot auto transpilation doesn't work correctly, a date_diff function
        must be implemented in the dialect, which will be used instead
        """

        if self.date_metric not in ("day", "month", "year"):
            raise ValueError("`date_metric` must be one of ('day', 'month', 'year')")

        sqlglot_dialect_name = sql_dialect.sqlglot_name
        date_col = InputColumn(self.col_name)
        date_col_l, date_col_r = date_col.names_l_r

        if hasattr(sql_dialect, "date_diff"):
            return sql_dialect.date_diff(self)

        if self.cast_strings_to_date:
            date_col_l = f"STR_TO_TIME({date_col_l}, '{self.date_format}')"
            date_col_r = f"STR_TO_TIME({date_col_r}, '{self.date_format}')"

        sqlglot_base_dialect_sql = (
            f"ABS(DATE_DIFF({date_col_l}, "
            f"{date_col_r}, '{self.date_metric}'))"
            f"<= {self.date_threshold}"
        )

        tree = parse_one(sqlglot_base_dialect_sql)

        return tree.sql(dialect=sqlglot_dialect_name)

    def create_label_for_charts(self) -> str:
        return (
            f"Date difference of '{self.col_name} <= "
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
        self.lat_col = lat_col
        self.long_col = long_col
        self.km_threshold = km_threshold
        self.not_null = not_null

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        lat_col_ic = input_column_factory(self.lat_col, splink_dialect=sql_dialect)
        long_col_ic = input_column_factory(self.long_col, splink_dialect=sql_dialect)
        lat_l, lat_r = lat_col_ic.names_l_r
        long_l, long_r = long_col_ic.names_l_r

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

        self.col_name = col_name
        self.min_intersection = min_intersection

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        if hasattr(sql_dialect, "array_intersect"):
            return sql_dialect.array_intersect(self)

        sqlglot_dialect_name = sql_dialect.sqlglot_name

        # Use undialected InputColumn here since it's being interpolated into
        # base dialected sql
        col = InputColumn(self.col_name)

        sqlglot_base_dialect_sql = f"""
            ARRAY_SIZE(ARRAY_INTERSECT({col.name_l}, {col.name_r}))
                >= {self.min_intersection}
                """
        tree = parse_one(sqlglot_base_dialect_sql)

        return tree.sql(dialect=sqlglot_dialect_name)

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

        self.col_name = col_name
        self.percentage_threshold = percentage_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = input_column_factory(self.col_name, splink_dialect=sql_dialect)
        return (
            f"(ABS({col.name_l} - {col.name_r}) / "
            f"(CASE "
            f"WHEN {col.name_r} > {col.name_l} THEN {col.name_r} "
            f"ELSE {col.name_l} "
            f"END)) < {self.percentage_threshold}"
        )

    def create_label_for_charts(self) -> str:
        return (
            f"Percentage difference of '{self.col_name}' "
            f"within {self.percentage_threshold:,.2%}"
        )

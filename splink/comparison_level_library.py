from typing import List, Union

from sqlglot import parse_one

from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect
from .input_column import InputColumn


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
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = self.input_column(sql_dialect)
        return f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL"

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
        super().__init__(col_name)
        self.configure(**config)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = self.input_column(sql_dialect)
        return f"{col.name_l()} = {col.name_r()}"

    def create_label_for_charts(self) -> str:
        return f"Exact match on {self.col_name}"


class LevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: int):
        """A comparison level using a sqlglot_dialect_name distance function

        e.g. levenshtein(val_l, val_r) <= distance_threshold

        Args:
            col_name (str): Input column name
            distance_threshold (int): The threshold to use to assess
                similarity
        """
        super().__init__(col_name)
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col = self.input_column(sql_dialect)
        lev_fn = sql_dialect.levenshtein_function_name
        return f"{lev_fn}({col.name_l()}, {col.name_r()}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return f"Levenshtein distance of {self.col_name} <= {self.distance_threshold}"


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
        super().__init__(col_name)
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
        date_col_l, date_col_r = date_col.names_l_r()

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
            f"Date difference of {self.col_name} <= "
            f"{self.date_threshold} {self.date_metric}"
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

        super().__init__(col_name)
        self.distance_threshold = validate_distance_threshold(
            lower_bound=0,
            upper_bound=1,
            distance_threshold=distance_threshold,
            level_name=self.__class__.__name__,
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        col_l, col_r = self.input_column(sql_dialect).names_l_r()
        jw_fn = sql_dialect.jaro_winkler_function_name
        return f"{jw_fn}({col_l}, {col_r}) >= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        return (
            f"Jaro-Winkler distance of '{self.col_name} >= {self.distance_threshold}'"
        )

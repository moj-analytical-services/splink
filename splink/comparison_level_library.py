from typing import Union

from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect
from .input_column import InputColumn


def input_column_factory(name, splink_dialect: SplinkDialect):
    return InputColumn(name, sql_dialect=splink_dialect.sqlglot_name)


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
        super().__init__(col_name)
        self.is_null_level = True

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
        # Refactor InputColumn so we don't need input_column_factory?
        # (The factory just slightly improves readability)

        input_column_1 = input_column_factory(self.col_name_1, sql_dialect)
        input_column_2 = input_column_factory(self.col_name_2, sql_dialect)

        return (
            f"{input_column_1.name_l()} = {input_column_2.name_r()} "
            f"AND {input_column_1.name_r()} = {input_column_2.name_l()}"
        )

    def create_label_for_charts(self) -> str:
        return f"Match on reversed cols: {self.col_name_1} and {self.col_name_2}"


class LevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str, distance_threshold: int):
        """A comparison level using a levenshtein distance function

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

from sqlglot import parse_one

from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect


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

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        sqlglot_dialect_name = sql_dialect.sqlglot_name()
        date_col = self.input_column(sql_dialect)
        date_col_l, date_col_r = date_col.names_l_r()
        col_l_no_dialect = parse_one(date_col_l, read=sqlglot_dialect_name).sql()
        col_r_no_dialect = parse_one(date_col_r, read=sqlglot_dialect_name).sql()

        if self.cast_strings_to_date:
            col_l_no_dialect = f'STR_TO_TIME({col_l_no_dialect}, "{self.date_format}")'
            col_r_no_dialect = f'STR_TO_TIME({col_r_no_dialect}, "{self.date_format}")'

        undialected_sql = (
            f"abs(date_diff({col_l_no_dialect}, "
            f'{col_r_no_dialect}, "{self.date_metric}")) '
            f"<= {self.date_threshold}"
        )

        query = parse_one(undialected_sql)

        return query.sql(dialect=sqlglot_dialect_name)

    def create_label_for_charts(self) -> str:
        return (
            f"Date difference of {self.col_name} <= "
            f"{self.date_threshold} {self.date_metric}"
        )

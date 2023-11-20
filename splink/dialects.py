from abc import ABC, abstractproperty
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .comparison_level_creator import ComparisonLevelCreator


class SplinkDialect(ABC):
    @abstractproperty
    def name(self):
        pass

    @property
    def sqlglot_name(self):
        return self.name

    @staticmethod
    def from_string(dialect_name: str):
        return _dialect_lookup[dialect_name]

    @property
    def str_to_date(self):
        return "str_to_time"

    @property
    def levenshtein_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Levenshtein' function"
        )

    @property
    def jaro_winkler_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Jaro-Winkler' function"
        )


class DuckDBDialect(SplinkDialect):
    @property
    def name(self):
        return "duckdb"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def jaro_winkler_function_name(self):
        return "jaro_winkler_similarity"


class SparkDialect(SplinkDialect):
    @property
    def name(self):
        return "spark"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def jaro_winkler_function_name(self):
        return "jaro_winkler"


class SqliteDialect(SplinkDialect):
    @property
    def name(self):
        return "sqlite"

    # SQLite does not natively support string distance functions.
    # However, sqlite UDFs are registered automatically by Splink
    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def jaro_winkler_function_name(self):
        return "jaro_winkler"


class PostgresDialect(SplinkDialect):
    @property
    def name(self):
        return "postgres"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def str_to_date(self):
        return "to_date"

    def date_diff(self, clc: "ComparisonLevelCreator", col_l: str, col_r: str) -> str:
        """Note some of these functions are not native postgres functions and
        instead are UDFs which are automatically registered by Splink
        """

        if clc.date_metric == "day":
            date_f = f"""
                abs(
                    datediff(
                        {col_l}, {col_r}
                    )
                )
            """
        elif clc.date_metric in ["month", "year"]:
            date_f = f"""
                floor(abs(
                    ave_months_between(
                        {col_l}, {col_r}
                    )"""
            if clc.date_metric == "year":
                date_f += " / 12))"
            else:
                date_f += "))"
        return f"""
            {date_f} <= {clc.date_threshold}
        """


class AthenaDialect(SplinkDialect):
    @property
    def name(self):
        return "athena"

    @property
    def sqlglot_name(self):
        return "presto"

    @property
    def _levenshtein_name(self):
        return "levenshtein_distance"


_dialect_lookup = {
    "duckdb": DuckDBDialect(),
    "spark": SparkDialect(),
    "sqlite": SqliteDialect(),
    "postgres": PostgresDialect(),
    "athena": AthenaDialect(),
}

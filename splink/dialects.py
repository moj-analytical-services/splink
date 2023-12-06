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
    def levenshtein_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Levenshtein' function"
        )

    @property
    def damerau_levenshtein_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Damerau-Levenshtein' function"
        )

    @property
    def jaro_winkler_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Jaro-Winkler' function"
        )

    @property
    def jaro_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Jaro' function"
        )

    @property
    def jaccard_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'Jaccard' function"
        )

    def try_parse_date(self, name: str, date_format: str = None):
        raise NotImplementedError(
            f"Backend '{self.name}' does not have a 'try_parse_date' function"
        )


class DuckDBDialect(SplinkDialect):
    @property
    def name(self):
        return "duckdb"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def damerau_levenshtein_function_name(self):
        return "damerau_levenshtein"

    @property
    def jaro_function_name(self):
        return "jaro_similarity"

    @property
    def jaro_winkler_function_name(self):
        return "jaro_winkler_similarity"

    @property
    def jaccard_function_name(self):
        return "jaccard"

    @property
    def default_date_foramt(self):
        return "%Y-%m-%d"

    def try_parse_date(self, name: str, date_format: str = None):
        if date_format is None:
            date_format = self.default_date_foramt
        return f"""try_strptime({name}, '{date_format}')"""


class SparkDialect(SplinkDialect):
    @property
    def name(self):
        return "spark"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def damerau_levenshtein_function_name(self):
        return "damerau_levenshtein"

    @property
    def jaro_function_name(self):
        return "jaro_sim"

    @property
    def jaro_winkler_function_name(self):
        return "jaro_winkler"

    @property
    def jaccard_function_name(self):
        return "jaccard"

    @property
    def default_date_foramt(self):
        return "yyyy-MM-dd"

    def try_parse_date(self, name: str, date_format: str = None):
        if date_format is None:
            date_format = self.default_date_foramt
        return f"""to_date({name}, '{date_format}')"""


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
    def damerau_levenshtein_function_name(self):
        return "damerau_levenshtein"

    @property
    def jaro_function_name(self):
        return "jaro_sim"

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

    def date_diff(self, clc: "ComparisonLevelCreator"):
        """Note some of these functions are not native postgres functions and
        instead are UDFs which are automatically registered by Splink
        """

        if clc.date_format is None:
            clc.date_format = "yyyy-MM-dd"

        clc.col_expression.sql_dialect = self
        col = clc.col_expression

        if clc.cast_strings_to_date:
            datediff_args = f"""
                to_date({col.name_l}, '{clc.date_format}'),
                to_date({col.name_r}, '{clc.date_format}')
            """
        else:
            datediff_args = f"{col.name_l}, {col.name_r}"

        if clc.date_metric == "day":
            date_f = f"""
                abs(
                    datediff(
                        {datediff_args}
                    )
                )
            """
        elif clc.date_metric in ["month", "year"]:
            date_f = f"""
                floor(abs(
                    ave_months_between(
                        {datediff_args}
                    )"""
            if clc.date_metric == "year":
                date_f += " / 12))"
            else:
                date_f += "))"
        return f"""
            {date_f} <= {clc.date_threshold}
        """

    def array_intersect(self, clc: "ComparisonLevelCreator"):
        clc.col_expression.sql_dialect = self
        col = clc.col_expression
        threshold = clc.min_intersection
        return f"""
        CARDINALITY(ARRAY_INTERSECT({col.name_l}, {col.name_r})) >= {threshold}
        """.strip()


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

    @property
    def default_date_foramt(self):
        return "%Y-%m-%d"

    def try_parse_date(self, name: str, date_format: str = None):
        if date_format is None:
            date_format = self.default_date_foramt
        return f"""try(date_parse({name}, '{date_format}'))"""


_dialect_lookup = {
    "duckdb": DuckDBDialect(),
    "spark": SparkDialect(),
    "sqlite": SqliteDialect(),
    "postgres": PostgresDialect(),
    "athena": AthenaDialect(),
}

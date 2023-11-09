from abc import ABC, abstractproperty


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
    # Instead, these operate as UDF sourced from 'fuzzyfinder'.
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

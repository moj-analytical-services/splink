from abc import ABC, abstractproperty
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .comparison_level_creator import ComparisonLevelCreator


class SplinkDialect(ABC):
    # Stores instances of each subclass of SplinkDialect.
    _dialect_instances = {}

    # Register a subclass of SplinkDialect on its creation.
    # Whenever that subclass is called again, use the previous instance.
    def __new__(cls, *args, **kwargs):
        if cls not in cls._dialect_instances:
            instance = super(SplinkDialect, cls).__new__(cls)
            cls._dialect_instances[cls] = instance
        return cls._dialect_instances[cls]

    @abstractproperty
    def name(self):
        pass

    @property
    def sqlglot_name(self):
        return self.name

    @classmethod
    def from_string(cls, dialect_name: str):
        # generator of classes which match _dialect_name_for_factory
        # should just get a single subclass, as this should be unique
        classes_from_dialect_name = (
            c for c in cls.__subclasses__()
            if c._dialect_name_for_factory == dialect_name
        )
        # use sequence unpacking to catch if we duplicate
        # _dialect_name_for_factory in subclasses
        [subclass] = classes_from_dialect_name
        return subclass()

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
    _dialect_name_for_factory = "duckdb"

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
    _dialect_name_for_factory = "spark"

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
    _dialect_name_for_factory = "sqlite"

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
    _dialect_name_for_factory = "postgres"

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

        col_name_l = clc.input_column(self).name_l()
        col_name_r = clc.input_column(self).name_r()

        if clc.cast_strings_to_date:
            datediff_args = f"""
                to_date({col_name_l}, '{clc.date_format}'),
                to_date({col_name_r}, '{clc.date_format}')
            """
        else:
            datediff_args = f"{col_name_l}, {col_name_r}"

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


class AthenaDialect(SplinkDialect):
    _dialect_name_for_factory = "athena"

    @property
    def name(self):
        return "athena"

    @property
    def sqlglot_name(self):
        return "presto"

    @property
    def _levenshtein_name(self):
        return "levenshtein_distance"

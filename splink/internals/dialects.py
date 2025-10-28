from __future__ import annotations

from abc import ABC, abstractproperty
from typing import TYPE_CHECKING, Literal, Type, TypeVar, final

if TYPE_CHECKING:
    from splink.internals.comparison_level_library import (
        AbsoluteTimeDifferenceLevel,
        ArrayIntersectLevel,
    )

# equivalent to typing.Self in python >= 3.11
Self = TypeVar("Self", bound="SplinkDialect")


class SplinkDialect(ABC):
    # Stores instances of each subclass of SplinkDialect.
    _dialect_instances: dict[Type[SplinkDialect], SplinkDialect] = {}
    # string defined by subclasses to be used in factory method from_string
    # give a dummy default value so that subclasses that fail to do this
    # don't ruin functionality for existing subclasses
    _dialect_name_for_factory: str

    # Register a subclass of SplinkDialect on its creation.
    # Whenever that subclass is called again, use the previous instance.
    def __new__(cls, *args, **kwargs):
        if cls not in cls._dialect_instances:
            instance = super(SplinkDialect, cls).__new__(cls)
            cls._dialect_instances[cls] = instance
        return cls._dialect_instances[cls]

    @abstractproperty
    def sql_dialect_str(self):
        pass

    @property
    def sqlglot_dialect(self):
        # If not explicitly set, return the splink_dialect_str
        # because they're usually the same except e.g. athena vs presto
        return self.sql_dialect_str

    @classmethod
    def from_string(cls: type[Self], dialect_name: str) -> Self:
        # list of classes which match _dialect_name_for_factory
        # should just get a single subclass, as this should be unique
        classes_from_dialect_name = [
            c
            for c in cls.__subclasses__()
            if getattr(c, "_dialect_name_for_factory", None) == dialect_name
        ]
        # use sequence unpacking to catch if we duplicate
        # _dialect_name_for_factory in subclasses
        if len(classes_from_dialect_name) == 1:
            subclass = classes_from_dialect_name[0]
            return subclass()
        # error - either too many subclasses found
        if len(classes_from_dialect_name) > 1:
            classes_string = ", ".join(map(str, classes_from_dialect_name))
            error_message = (
                "Found multiple subclasses of `SplinkDialect` with "
                "lookup string `_dialect_name_for_factory` equal to "
                f"supplied value {dialect_name}: {classes_string}!"
            )
        # or _no_ subclasses found
        else:
            error_message = (
                "Could not find subclass of `SplinkDialect` with "
                f"lookup string '{dialect_name}'."
            )
        raise ValueError(error_message)

    @property
    def levenshtein_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'Levenshtein' function"
        )

    @property
    def damerau_levenshtein_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'Damerau-Levenshtein' function"
        )

    @property
    def jaro_winkler_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'Jaro-Winkler' function"
        )

    @property
    def jaro_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a 'Jaro' function"
        )

    @property
    def jaccard_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a 'Jaccard' function"
        )

    @property
    def cosine_similarity_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'Cosine Similarity' function"
        )

    @property
    def array_max_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have an 'Array max' function"
        )

    @property
    def array_min_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have an 'Array min' function"
        )

    @property
    def array_transform_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have an "
            "'Array transform' function"
        )

    @property
    def array_first_index(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "first array index defined"
        )

    @property
    def greatest_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a " "'Greatest' function"
        )

    @property
    def least_function_name(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a " "'Least' function"
        )

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' needs a random_sample_sql "
            "added to its dialect"
        )

    @property
    def infinity_expression(self):
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' needs an infinity_expression "
            "added to its dialect"
        )

    @staticmethod
    def _wrap_in_nullif(func):
        def nullif_wrapped_function(*args, **kwargs):
            # convert empty strings to NULL
            return f"NULLIF({func(*args, **kwargs)}, '')"

        return nullif_wrapped_function

    def try_parse_date(self, name: str, date_format: str = None) -> str:
        return self._try_parse_date_raw(name, date_format)

    def _try_parse_date_raw(self, name: str, date_format: str = None) -> str:
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'try_parse_date' function"
        )

    def try_parse_timestamp(self, name: str, timestamp_format: str = None) -> str:
        return self._try_parse_timestamp_raw(name, timestamp_format)

    def _try_parse_timestamp_raw(self, name: str, timestamp_format: str = None) -> str:
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'try_parse_timestamp' function"
        )

    @final
    def regex_extract(self, name: str, pattern: str, capture_group: int = 0) -> str:
        return self._wrap_in_nullif(self._regex_extract_raw)(
            name, pattern, capture_group
        )

    def _regex_extract_raw(
        self, name: str, pattern: str, capture_group: int = 0
    ) -> str:
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have a "
            "'regex_extract' function"
        )

    def access_extreme_array_element(
        self, name: str, first_or_last: Literal["first", "last"]
    ) -> str:
        raise NotImplementedError(
            f"Backend '{self.sql_dialect_str}' does not have an "
            "'access_extreme_array_element' function"
        )

    def explode_arrays_sql(
        self,
        tbl_name: str,
        columns_to_explode: list[str],
        other_columns_to_retain: list[str],
    ) -> str:
        raise NotImplementedError(
            f"Unnesting blocking rules are not supported for {type(self)}"
        )


class DuckDBDialect(SplinkDialect):
    _dialect_name_for_factory = "duckdb"

    @property
    def sql_dialect_str(self):
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
    def array_max_function_name(self):
        return "list_max"

    @property
    def array_min_function_name(self):
        return "list_min"

    @property
    def array_transform_function_name(self):
        return "list_transform"

    @property
    def array_first_index(self):
        return 1

    @property
    def greatest_function_name(self):
        return "greatest"

    @property
    def least_function_name(self):
        return "least"

    @property
    def default_date_format(self):
        return "%Y-%m-%d"

    @property
    def default_timestamp_format(self):
        return "%Y-%m-%dT%H:%M:%SZ"

    def _try_parse_date_raw(self, name: str, date_format: str = None) -> str:
        if date_format is None:
            date_format = self.default_date_format
        return f"""try_strptime({name}, '{date_format}')"""

    def _try_parse_timestamp_raw(self, name: str, timestamp_format: str = None) -> str:
        if timestamp_format is None:
            timestamp_format = self.default_timestamp_format
        return f"""try_strptime({name}, '{timestamp_format}')"""

    def array_intersect(self, clc: ArrayIntersectLevel) -> str:
        clc.col_expression.sql_dialect = self
        col = clc.col_expression
        thres = clc.min_intersection
        return f"array_length(list_intersect({col.name_l}, {col.name_r})) >= {thres}"

    def _regex_extract_raw(
        self, name: str, pattern: str, capture_group: int = 0
    ) -> str:
        return f"regexp_extract({name}, '{pattern}', {capture_group})"

    @property
    def infinity_expression(self):
        return "cast('infinity' as float8)"

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        if seed:
            return f"USING SAMPLE bernoulli({percent}%) REPEATABLE({seed})"
        else:
            return f"USING SAMPLE {percent}% (bernoulli)"

    def access_extreme_array_element(
        self, name: str, first_or_last: Literal["first", "last"]
    ) -> str:
        if first_or_last == "first":
            return f"{name}[{self.array_first_index}]"
        if first_or_last == "last":
            return f"{name}[-1]"
        raise ValueError(
            f"Argument 'first_or_last' should be 'first' or 'last', "
            f"received: '{first_or_last}'"
        )

    def explode_arrays_sql(
        self,
        tbl_name: str,
        columns_to_explode: list[str],
        other_columns_to_retain: list[str],
    ) -> str:
        """Generated sql that explodes one or more columns in a table"""
        columns_to_explode = columns_to_explode.copy()
        other_columns_to_retain = other_columns_to_retain.copy()
        # base case
        if len(columns_to_explode) == 0:
            return f"select {','.join(other_columns_to_retain)} from {tbl_name}"
        else:
            column_to_explode = columns_to_explode.pop()
            cols_to_select = (
                [f"unnest({column_to_explode}) as {column_to_explode}"]
                + other_columns_to_retain
                + columns_to_explode
            )
            other_columns_to_retain.append(column_to_explode)
            return f"""select {','.join(cols_to_select)}
                from ({self.explode_arrays_sql(tbl_name,columns_to_explode,other_columns_to_retain)})"""  # noqa: E501

    @property
    def cosine_similarity_function_name(self):
        return "array_cosine_similarity"


class SparkDialect(SplinkDialect):
    _dialect_name_for_factory = "spark"

    @property
    def sql_dialect_str(self):
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
    def array_max_function_name(self):
        return "array_max"

    @property
    def array_min_function_name(self):
        return "array_min"

    @property
    def array_transform_function_name(self):
        return "transform"

    @property
    def array_first_index(self):
        return 0

    @property
    def greatest_function_name(self):
        return "greatest"

    @property
    def least_function_name(self):
        return "least"

    @property
    def default_date_format(self):
        return "yyyy-MM-dd"

    @property
    def default_timestamp_format(self):
        return "yyyy-MM-dd\\'T\\'HH:mm:ssXXX"

    def _try_parse_date_raw(self, name: str, date_format: str = None) -> str:
        if date_format is None:
            date_format = self.default_date_format
        return f"""date(try_to_timestamp({name}, '{date_format}'))"""

    def _try_parse_timestamp_raw(self, name: str, timestamp_format: str = None) -> str:
        if timestamp_format is None:
            timestamp_format = self.default_timestamp_format
        return f"""try_to_timestamp({name}, '{timestamp_format}')"""

    def _regex_extract_raw(
        self, name: str, pattern: str, capture_group: int = 0
    ) -> str:
        return f"regexp_extract({name}, '{pattern}', {capture_group})"

    @property
    def infinity_expression(self):
        return "'infinity'"

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        if seed:
            return f" ORDER BY rand({seed}) LIMIT {round(sample_size)}"
        else:
            return f" TABLESAMPLE ({percent} PERCENT) "

    def access_extreme_array_element(
        self, name: str, first_or_last: Literal["first", "last"]
    ) -> str:
        if first_or_last == "first":
            return f"{name}[{self.array_first_index}]"
        if first_or_last == "last":
            return f"element_at({name}, -1)"
        raise ValueError(
            f"Argument 'first_or_last' should be 'first' or 'last', "
            f"received: '{first_or_last}'"
        )

    def explode_arrays_sql(
        self,
        tbl_name: str,
        columns_to_explode: list[str],
        other_columns_to_retain: list[str],
    ) -> str:
        """Generated sql that explodes one or more columns in a table"""
        columns_to_explode = columns_to_explode.copy()
        other_columns_to_retain = other_columns_to_retain.copy()
        if len(columns_to_explode) == 0:
            return f"select {','.join(other_columns_to_retain)} from {tbl_name}"
        else:
            column_to_explode = columns_to_explode.pop()
            cols_to_select = (
                [f"explode({column_to_explode}) as {column_to_explode}"]
                + other_columns_to_retain
                + columns_to_explode
            )
        return f"""select {','.join(cols_to_select)}
                from ({self.explode_arrays_sql(tbl_name,columns_to_explode,other_columns_to_retain+[column_to_explode])})"""  # noqa: E501


class SQLiteDialect(SplinkDialect):
    _dialect_name_for_factory = "sqlite"

    @property
    def sql_dialect_str(self):
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

    @property
    def infinity_expression(self):
        return "'infinity'"

    @property
    def greatest_function_name(self):
        # SQLite uses min/max scalar functions instead of least/greatest
        return "max"

    @property
    def least_function_name(self):
        return "min"

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        if seed:
            raise NotImplementedError(
                "SQLite does not support seeds in random ",
                "samples. Please remove the `seed` parameter.",
            )

        sample_size = int(sample_size)

        return f"""ORDER BY RANDOM()
            LIMIT {sample_size}
            """


class PostgresDialect(SplinkDialect):
    _dialect_name_for_factory = "postgres"

    @property
    def sql_dialect_str(self):
        return "postgres"

    @property
    def levenshtein_function_name(self):
        return "levenshtein"

    @property
    def greatest_function_name(self):
        return "greatest"

    @property
    def least_function_name(self):
        return "least"

    def absolute_time_difference(self, clc: AbsoluteTimeDifferenceLevel) -> str:
        # need custom solution as sqlglot gets confused by 'metric', as in Spark
        # datediff _only_ works in days
        clc.col_expression.sql_dialect = self
        col = clc.col_expression

        return (
            f"ABS(EXTRACT(EPOCH FROM {col.name_l}) "
            f"- EXTRACT(EPOCH FROM {col.name_r}))"
            f"<= {clc.time_threshold_seconds}"
        )

    def _regex_extract_raw(
        self, name: str, pattern: str, capture_group: int = 0
    ) -> str:
        # full match - wrap pattern in parentheses so first group is whole expression
        if capture_group == 0:
            pattern = f"({pattern})"
        if capture_group > 1:
            # currently no easy way to capture non-first groups
            raise ValueError(
                "'postgres' backend does not currently support a capture_group greater "
                "than 1. To proceed you must use your own SQL expression"
            )
        return f"substring({name} from '{pattern}')"

    @property
    def default_date_format(self):
        return "YYYY-MM-DD"

    @property
    def default_timestamp_format(self):
        return "YYYY-MM-DDTHH24:MI:SS"

    def try_parse_date(self, name: str, date_format: str = None) -> str:
        if date_format is None:
            date_format = self.default_date_format
        return f"""try_cast_date({name}, '{date_format}')"""

    def try_parse_timestamp(self, name: str, timestamp_format: str = None) -> str:
        if timestamp_format is None:
            timestamp_format = self.default_timestamp_format
        return f"""try_cast_timestamp({name}, '{timestamp_format}')"""

    def array_intersect(self, clc: ArrayIntersectLevel) -> str:
        clc.col_expression.sql_dialect = self
        col = clc.col_expression
        threshold = clc.min_intersection
        return f"""
        CARDINALITY(ARRAY_INTERSECT({col.name_l}, {col.name_r})) >= {threshold}
        """.strip()

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        if seed:
            # TODO: we could maybe do seeds by handling it in calling function
            # need to execute setseed() in surrounding session
            raise NotImplementedError(
                "Postgres does not support seeds in random "
                "samples. Please remove the `seed` parameter."
            )

        sample_size = int(sample_size)

        return f"""ORDER BY RANDOM()
            LIMIT {sample_size}
            """

    @property
    def infinity_expression(self):
        return "'infinity'"

    @property
    def array_first_index(self):
        return 1

    def access_extreme_array_element(
        self, name: str, first_or_last: Literal["first", "last"]
    ) -> str:
        if first_or_last == "first":
            return f"{name}[{self.array_first_index}]"
        if first_or_last == "last":
            return f"{name}[array_length({name}, 1)]"
        raise ValueError(
            f"Argument 'first_or_last' should be 'first' or 'last', "
            f"received: '{first_or_last}'"
        )


class AthenaDialect(SplinkDialect):
    _dialect_name_for_factory = "athena"

    @property
    def sql_dialect_str(self):
        return "athena"

    @property
    def sqlglot_dialect(self):
        return "presto"

    @property
    def _levenshtein_name(self):
        return "levenshtein_distance"

    def random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        if proportion == 1.0:
            return ""
        percent = proportion * 100
        if seed:
            return f"USING SAMPLE bernoulli({percent}%) REPEATABLE({seed})"
        else:
            return f"USING SAMPLE {percent}% (bernoulli)"

    @property
    def infinity_expression(self):
        return "infinity()"

    @property
    def levenshtein_function_name(self):
        return "levenshtein_distance"

    @property
    def greatest_function_name(self):
        return "greatest"

    @property
    def least_function_name(self):
        return "least"

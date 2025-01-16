from __future__ import annotations

from copy import copy
from functools import wraps
from typing import Any, Callable, List, Literal, TypeVar, Union

from sqlglot import TokenError, parse_one

from splink.internals.column_expression import ColumnExpression
from splink.internals.comparison_level_sql import great_circle_distance_km_sql
from splink.internals.dialects import SplinkDialect

# import composition functions for export
from .comparison_level_composition import And, Not, Or  # NOQA: F401
from .comparison_level_creator import ComparisonLevelCreator

# type aliases:
T = TypeVar("T", bound=ComparisonLevelCreator)
CreateSQLFunctionType = Callable[[T, SplinkDialect], str]


def unsupported_splink_dialects(
    unsupported_dialects: List[str],
) -> Callable[[CreateSQLFunctionType[T]], CreateSQLFunctionType[T]]:
    def decorator(func: CreateSQLFunctionType[T]) -> CreateSQLFunctionType[T]:
        @wraps(func)
        def wrapper(self: T, splink_dialect: SplinkDialect) -> str:
            if splink_dialect.sql_dialect_str in unsupported_dialects:
                raise ValueError(
                    f"Dialect {splink_dialect.sql_dialect_str} is not supported "
                    f"for {self.__class__.__name__}"
                )
            return func(self, splink_dialect)

        return wrapper

    return decorator


def _translate_sql_string(
    sqlglot_base_dialect_sql: str,
    to_sqlglot_dialect: str,
    from_sqlglot_dialect: str = None,
) -> str:
    tree = parse_one(sqlglot_base_dialect_sql, read=from_sqlglot_dialect)

    return tree.sql(dialect=to_sqlglot_dialect)


def validate_numeric_parameter(
    lower_bound: Union[int, float],
    upper_bound: Union[int, float],
    parameter_value: Union[int, float],
    level_name: str,
    parameter_name: str = "distance_threshold",
) -> Union[int, float]:
    """Check if a distance threshold falls between two bounds."""
    if not isinstance(parameter_value, (int, float)):
        raise TypeError(
            f"'{parameter_name}' must be numeric, but received type "
            f"{type(parameter_value)}"
        )
    if lower_bound <= parameter_value <= upper_bound:
        return parameter_value
    else:
        raise ValueError(
            f"'{parameter_name}' must be between "
            f"{lower_bound} and {upper_bound} for {level_name}"
        )


def validate_categorical_parameter(
    allowed_values: List[str],
    parameter_value: str,
    level_name: str,
    parameter_name: str,
) -> str:
    """Check if a distance threshold falls between two bounds."""
    if parameter_value in allowed_values:
        return parameter_value
    else:
        comma_quote_separated_options = "', '".join(allowed_values)
        raise ValueError(
            f"'{parameter_name}' must be one of: " f"'{comma_quote_separated_options}'"
        )


class NullLevel(ComparisonLevelCreator):
    """Represents a comparison level where either or both values are NULL

    e.g. val_l IS NULL OR val_r IS NULL

    Args:
        col_name (Union[str, ColumnExpression]): Input column name or ColumnExpression
        valid_string_pattern (str, optional): If provided, a regex pattern to extract
            a valid substring from the column before checking for NULL. Default is None.

    Note:
        If a valid_string_pattern is provided, the NULL check will be performed on
        the extracted substring rather than the original column value.
    """

    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        valid_string_pattern: str = None,
    ):
        col_expression = ColumnExpression.instantiate_if_str(col_name)

        if valid_string_pattern is not None:
            col_expression = col_expression.regex_extract(valid_string_pattern)
        self.col_expression = col_expression
        self.is_null_level = True

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        null_sql = f"{col.name_l} IS NULL OR {col.name_r} IS NULL"
        return null_sql

    def create_label_for_charts(self) -> str:
        return f"{self.col_expression.label} is NULL"


class ElseLevel(ComparisonLevelCreator):
    """
    This level is used to capture all comparisons that do not match any other
    specified levels. It corresponds to the ELSE clause in a SQL CASE statement.
    """

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
                base_dialect_sqlglot_name = base_dialect.sqlglot_dialect

                # as default, translate condition into our dialect
                try:
                    sql_condition = _translate_sql_string(
                        sql_condition,
                        sql_dialect.sqlglot_dialect,
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

    @staticmethod
    def _convert_to_creator(
        cl: Union[ComparisonLevelCreator, dict[str, Any]],
    ) -> ComparisonLevelCreator:
        if isinstance(cl, ComparisonLevelCreator):
            return cl
        if isinstance(cl, dict):
            # TODO: swap this if we develop a more uniform approach to (de)serialising
            cl_dict = copy(cl)
            configurable_parameters = (
                "is_null_level",
                "m_probability",
                "u_probability",
                "tf_adjustment_column",
                "tf_adjustment_weight",
                "tf_minimum_u_value",
                "label_for_charts",
                "disable_tf_exact_match_detection",
                "fix_m_probability",
                "fix_u_probability",
            )
            # split dict in two depending whether or not entries are 'configurables'
            configurables = {
                key: value
                for key, value in cl_dict.items()
                if key in configurable_parameters
            }
            cl_dict = {
                key: value
                for key, value in cl_dict.items()
                if key not in configurable_parameters
            }

            custom_comparison = CustomLevel(**cl_dict)
            if configurables:
                custom_comparison.configure(**configurables)
            return custom_comparison
        raise ValueError(
            "`comparison_levels` entries must be `dict` or `ComparisonLevelCreator, "
            f"but found type {type(cl)} for entry {cl}"
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
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.term_frequency_adjustments = term_frequency_adjustments
        self.is_exact_match_level = True

    @property
    def term_frequency_adjustments(self):
        # mypy doesn't know about attribute as we use magic in .configure()
        return self.tf_adjustment_column is not None  # type: ignore [attr-defined]

    @term_frequency_adjustments.setter
    def term_frequency_adjustments(self, term_frequency_adjustments: bool) -> None:
        if term_frequency_adjustments:
            if not self.col_expression.is_pure_column_or_column_reference:
                raise ValueError(
                    "The boolean term_frequency_adjustments argument"
                    " can only be used if the column name has no "
                    "transforms applied to it such as lower(), "
                    "substr() etc."
                )
            # leave tf_minimum_u_value as None
            # Since we know that it's a pure column reference it's fine to assign the
            # raw unescaped value to the dict - it will be processed via `InputColumn`
            # when the dict is read

            self.configure(
                tf_adjustment_column=self.col_expression.raw_sql_expression,
                tf_adjustment_weight=1.0,
            )
        else:
            self.configure(
                tf_adjustment_column=None,
                tf_adjustment_weight=None,
            )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"{col.name_l} = {col.name_r}"

    def create_label_for_charts(self) -> str:
        return f"Exact match on {self.col_expression.label}"


class LiteralMatchLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        literal_value: str,
        literal_datatype: str,
        side_of_comparison: str = "both",
    ):
        """Represents a comparison level where a column matches a literal value

        e.g. val_l = 'literal' AND/OR val_r = 'literal'

        Args:
            col_name (Union[str, ColumnExpression]): Input column name or
                ColumnExpression
            literal_value (str): The literal value to compare against e.g. 'male'
            literal_datatype (str): The datatype of the literal value.
                Must be one of: "string", "int", "float", "date"
            side_of_comparison (str, optional): Which side(s) of the comparison to
                apply. Must be one of: "left", "right", "both". Defaults to "both".
        """
        self.side_of_comparison = validate_categorical_parameter(
            allowed_values=["left", "right", "both"],
            parameter_value=side_of_comparison,
            level_name=self.__class__.__name__,
            parameter_name="side_of_comparison",
        )

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.literal_value_undialected = literal_value

        self.literal_datatype = validate_categorical_parameter(
            allowed_values=["string", "int", "float", "date"],
            parameter_value=literal_datatype,
            level_name=self.__class__.__name__,
            parameter_name="literal_datatype",
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        dialect = sql_dialect.sqlglot_dialect
        lit = self.literal_value_undialected

        if self.literal_datatype == "string":
            dialected = parse_one(f"'{lit}'").sql(dialect)
        elif self.literal_datatype == "date":
            dialected = parse_one(f"cast('{lit}' as date)").sql(dialect)
        elif self.literal_datatype == "int":
            dialected = parse_one(f"cast({lit} as int)").sql(dialect)
        elif self.literal_datatype == "float":
            dialected = parse_one(f"cast({lit} as float)").sql(dialect)

        if self.side_of_comparison == "left":
            return f"{col.name_l} = {dialected}"
        elif self.side_of_comparison == "right":
            return f"{col.name_r} = {dialected}"
        elif self.side_of_comparison == "both":
            return f"{col.name_l} = {dialected}" f" AND {col.name_r} = {dialected}"
        raise ValueError(f"Invalid `side_of_comparison`: {self.side_of_comparison}.")

    def create_label_for_charts(self) -> str:
        return (
            f"{self.col_expression.label} = {self.literal_value_undialected} "
            f"on {self.side_of_comparison}"
        )


class ColumnsReversedLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name_1: Union[str, ColumnExpression],
        col_name_2: Union[str, ColumnExpression],
        symmetrical: bool = False,
    ):
        """Represents a comparison level where the columns are reversed. For example,
        if surname is in the forename field and vice versa

        By default, col_l = col_r.  If the symmetrical argument is True, then
        col_l = col_r AND col_r = col_l.

        Args:
            col_name_1 (str): First column, e.g. forename
            col_name_2 (str): Second column, e.g. surname
            symmetrical (bool): If True, equality is required in in both directions.
                Default is False.
        """
        self.col_expression_1 = ColumnExpression.instantiate_if_str(col_name_1)
        self.col_expression_2 = ColumnExpression.instantiate_if_str(col_name_2)
        self.symmetrical = symmetrical

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression_1.sql_dialect = sql_dialect
        self.col_expression_2.sql_dialect = sql_dialect
        col_1 = self.col_expression_1
        col_2 = self.col_expression_2

        if self.symmetrical:
            return (
                f"{col_1.name_l} = {col_2.name_r} AND {col_1.name_r} = {col_2.name_l}"
            )
        else:
            return f"{col_1.name_l} = {col_2.name_r}"

    def create_label_for_charts(self) -> str:
        col_1 = self.col_expression_1
        col_2 = self.col_expression_2
        direction = "both directions" if self.symmetrical else "one direction"
        return f"Match on reversed cols: {col_1.label} and {col_2.label} ({direction})"


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

    def create_label_for_charts(self) -> str:
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
        self.distance_threshold = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=1,
            parameter_value=distance_threshold,
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
        self.distance_threshold = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=1,
            parameter_value=distance_threshold,
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
        self.distance_threshold = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=1,
            parameter_value=distance_threshold,
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


class DistanceFunctionLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        distance_function_name: str,
        distance_threshold: Union[int, float],
        higher_is_more_similar: bool = True,
    ):
        """A comparison level using an arbitrary distance function

        e.g. `custom_distance(val_l, val_r) >= (<=) distance_threshold`

        The function given by `distance_function_name` must exist in the SQL
        backend you use, and must take two parameters of the type in `col_name,
        returning a numeric type

        Args:
            col_name (str | ColumnExpression): Input column name
            distance_function_name (str): the name of the SQL distance function
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            higher_is_more_similar (bool): Are higher values of the distance function
                more similar? (e.g. True for Jaro-Winkler, False for Levenshtein)
                Default is True
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_function_name = distance_function_name
        self.distance_threshold = distance_threshold
        self.higher_is_more_similar = higher_is_more_similar

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        d_fn = self.distance_function_name
        less_or_greater_than = ">" if self.higher_is_more_similar else "<"
        return (
            f"{d_fn}({col.name_l}, {col.name_r}) "
            f"{less_or_greater_than}= {self.distance_threshold}"
        )

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        less_or_greater = "greater" if self.higher_is_more_similar else "less"
        return (
            f"`{self.distance_function_name}` distance of '{col.label} "
            f"{less_or_greater} than {self.distance_threshold}'"
        )


class PairwiseStringDistanceFunctionLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: str | ColumnExpression,
        distance_function_name: Literal[
            "levenshtein", "damerau_levenshtein", "jaro_winkler", "jaro"
        ],
        distance_threshold: Union[int, float],
    ):
        """A comparison level using the *most similar* string distance
        between any pair of values between arrays in an array column.

        The function given by `distance_function_name` must be one of
        "levenshtein," "damera_levenshtein," "jaro_winkler," or "jaro."

        Args:
            col_name (str | ColumnExpression): Input column name
            distance_function_name (str): the name of the string distance function
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_function_name = validate_categorical_parameter(
            allowed_values=[
                "levenshtein",
                "damerau_levenshtein",
                "jaro_winkler",
                "jaro",
            ],
            parameter_value=distance_function_name,
            level_name=self.__class__.__name__,
            parameter_name="distance_function_name",
        )
        self.distance_threshold = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=float("inf"),
            parameter_value=distance_threshold,
            level_name=self.__class__.__name__,
            parameter_name="distance_threshold",
        )

    @unsupported_splink_dialects(["sqlite", "postgres", "athena"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        distance_function_name_transpiled = {
            "levenshtein": sql_dialect.levenshtein_function_name,
            "damerau_levenshtein": sql_dialect.damerau_levenshtein_function_name,
            "jaro_winkler": sql_dialect.jaro_winkler_function_name,
            "jaro": sql_dialect.jaro_function_name,
        }[self.distance_function_name]

        aggregator_func = {
            "min": sql_dialect.array_min_function_name,
            "max": sql_dialect.array_max_function_name,
        }[self._aggregator()]

        return f"""{aggregator_func}(
                    {sql_dialect.array_transform_function_name}(
                        flatten(
                            {sql_dialect.array_transform_function_name}(
                                {col.name_l},
                                x -> {sql_dialect.array_transform_function_name}(
                                    {col.name_r},
                                    y -> [x, y]
                                )
                            )
                        ),
                        pair -> {distance_function_name_transpiled}(
                            pair[{sql_dialect.array_first_index}],
                            pair[{sql_dialect.array_first_index + 1}]
                        )
                    )
                ) {self._comparator()} {self.distance_threshold}"""

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return (
            f"{self._aggregator().title()} `{self.distance_function_name}` "
            f"distance of '{col.label}' "
            f"{self._comparator()} than {self.distance_threshold}'"
        )

    def _aggregator(self):
        return "max" if self._higher_is_more_similar() else "min"

    def _comparator(self):
        return ">=" if self._higher_is_more_similar() else "<="

    def _higher_is_more_similar(self):
        return {
            "levenshtein": False,
            "damerau_levenshtein": False,
            "jaro_winkler": True,
            "jaro": True,
        }[self.distance_function_name]


DateMetricType = Literal["second", "minute", "hour", "day", "month", "year"]


class AbsoluteTimeDifferenceLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        input_is_string: bool,
        threshold: Union[int, float],
        metric: DateMetricType,
        datetime_format: str = None,
    ):
        """
        Computes the absolute elapsed time between two dates (total duration).

        This function computes the amount of time that has passed between two dates,
        in contrast to functions like `date_diff` found in some SQL backends,
        which count the number of full calendar intervals (e.g., months, years) crossed.

        For instance, the difference between January 29th and March 2nd would be less
        than two months in terms of elapsed time, unlike a `date_diff` calculation that
        would give an answer of 2 calendar intervals crossed.

        That the thresold is inclusive e.g. a level with a 10 day threshold
        will include difference in date of 10 days.

        Args:
            col_name (str): The name of the input column containing the dates to compare
            input_is_string (bool): Indicates if the input date/times are in
                string format, requiring parsing according to `datetime_format`.
            threshold (int): The maximum allowed difference between the two dates,
                in units specified by `date_metric`.
            metric (str): The unit of time to use when comparing the dates.
                Can be 'second', 'minute', 'hour', 'day', 'month', or 'year'.
            datetime_format (str, optional): The format string for parsing dates.
                ISO 8601 format used if not provided.


        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.time_threshold_raw = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=float("inf"),
            parameter_value=threshold,
            level_name=self.__class__.__name__,
            parameter_name="threshold",
        )
        self.time_metric = validate_categorical_parameter(
            allowed_values=["second", "minute", "hour", "day", "month", "year"],
            parameter_value=metric,
            level_name=self.__class__.__name__,
            parameter_name="metric",
        )

        self.time_threshold_seconds = self.convert_time_metric_to_seconds(
            self.time_threshold_raw, self.time_metric
        )

        self.datetime_format = datetime_format
        self.input_is_string = input_is_string

    def convert_time_metric_to_seconds(self, threshold: float, metric: str) -> float:
        conversion_factors = {
            "second": 1,
            "minute": 60,
            "hour": 60 * 60,
            "day": 60 * 60 * 24,
            "month": 60 * 60 * 24 * 365.25 / 12,
            "year": 60 * 60 * 24 * 365.25,
        }
        return threshold * conversion_factors[metric]

    @property
    def datetime_parsed_column_expression(self):
        return self.col_expression.try_parse_timestamp

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        """Use sqlglot to auto transpile where possible
        Where sqlglot auto transpilation doesn't work correctly, a date_diff function
        must be implemented in the dialect, which will be used instead
        """

        self.col_expression.sql_dialect = sql_dialect

        if self.input_is_string:
            self.col_expression = self.datetime_parsed_column_expression(
                self.datetime_format
            )

        # If the dialect has an override, use it
        if hasattr(sql_dialect, "absolute_time_difference"):
            return sql_dialect.absolute_time_difference(self)

        sqlglot_base_dialect_sql = (
            "abs(TIME_TO_UNIX(___col____l)"
            " - TIME_TO_UNIX(___col____r))"
            f"<= {self.time_threshold_seconds}"
        )

        sqlglot_dialect_name = sql_dialect.sqlglot_dialect
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
            f"Abs difference of '{col.label} <= "
            f"{self.time_threshold_raw} {self.time_metric}'"
        )


class AbsoluteDateDifferenceLevel(AbsoluteTimeDifferenceLevel):
    @property
    def datetime_parsed_column_expression(self):
        return self.col_expression.try_parse_date


class DistanceInKMLevel(ComparisonLevelCreator):
    def __init__(
        self,
        lat_col: str | ColumnExpression,
        long_col: str | ColumnExpression,
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


class CosineSimilarityLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        similarity_threshold: float,
    ):
        """A comparison level using a cosine similarity function

        e.g. array_cosine_similarity(val_l, val_r) >= similarity_threshold

        Args:
            col_name (str): Input column name
            similarity_threshold (float): The threshold to use to assess
                similarity. Should be between 0 and 1.
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.similarity_threshold = validate_numeric_parameter(
            lower_bound=0.0,
            upper_bound=1.0,
            parameter_value=similarity_threshold,
            level_name=self.__class__.__name__,
            parameter_name="similarity_threshold",
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        cs_fn = sql_dialect.cosine_similarity_function_name
        return f"{cs_fn}({col.name_l}, {col.name_r}) >= {self.similarity_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Cosine similarity of {col.label} >= {self.similarity_threshold}"


class ArrayIntersectLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str | ColumnExpression, min_intersection: int = 1):
        """Represents a comparison level based around the size of an intersection of
        arrays

        Args:
            col_name (str): Input column name
            min_intersection (int, optional): The minimum cardinality of the
                intersection of arrays for this comparison level. Defaults to 1
        """

        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.min_intersection = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=float("inf"),
            parameter_value=min_intersection,
            level_name=self.__class__.__name__,
            parameter_name="min_intersection",
        )

    @unsupported_splink_dialects(["sqlite"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        if hasattr(sql_dialect, "array_intersect"):
            return sql_dialect.array_intersect(self)

        sqlglot_dialect_name = sql_dialect.sqlglot_dialect

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


class ArraySubsetLevel(ComparisonLevelCreator):
    def __init__(self, col_name: str | ColumnExpression, empty_is_subset: bool = False):
        """Represents a comparison level where the smaller array is an
        exact subset of the larger array. If arrays are equal length, they
        must have the same elements

        The order of items in the arrays does not matter for this comparison.

        Args:
            col_name (str | ColumnExpression): Input column name or ColumnExpression
            empty_is_subset (bool): If True, an empty array is considered a subset of
                any array (including another empty array). Default is False.
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.empty_is_subset = empty_is_subset

    # Postgres not supported since it doesn't correctly deal with zero length arrays
    @unsupported_splink_dialects(["sqlite", "postgres"])
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        sqlglot_dialect_name = sql_dialect.sqlglot_dialect

        empty_check = ""
        if not self.empty_is_subset:
            empty_check = (
                "LEAST(ARRAY_SIZE(___col____l), ARRAY_SIZE(___col____r)) <> 0 AND"
            )

        sqlglot_base_dialect_sql = f"""
            {empty_check}
            ARRAY_SIZE(ARRAY_INTERSECT(___col____l, ___col____r)) =
            LEAST(ARRAY_SIZE(___col____l), ARRAY_SIZE(___col____r))
            """
        translated = _translate_sql_string(
            sqlglot_base_dialect_sql, sqlglot_dialect_name
        )

        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        translated = translated.replace("___col____l", col.name_l)
        translated = translated.replace("___col____r", col.name_r)
        return translated

    def create_label_for_charts(self) -> str:
        return "Array subset"


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


class AbsoluteDifferenceLevel(ComparisonLevelCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        difference_threshold: Union[int, float],
    ):
        """
        Represents a comparison level where the absolute difference between two
        numerical values is within a specified threshold.

        Args:
            col_name (str | ColumnExpression): Input column name or ColumnExpression.
            difference_threshold (int | float): The maximum allowed absolute difference
                between the two values.
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.difference_threshold = validate_numeric_parameter(
            lower_bound=0,
            upper_bound=float("inf"),
            parameter_value=difference_threshold,
            level_name=self.__class__.__name__,
            parameter_name="difference_threshold",
        )

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"ABS({col.name_l} - {col.name_r}) <= {self.difference_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Absolute difference of '{col.label}' <= {self.difference_threshold}"

from __future__ import annotations

from typing import Any, Iterable, List, Literal, Optional, Union

from splink.internals import comparison_level_library as cll
from splink.internals.column_expression import ColumnExpression
from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.comparison_level_library import CustomLevel, DateMetricType
from splink.internals.dialects import SplinkDialect
from splink.internals.misc import ensure_is_iterable


class ExactMatch(ComparisonCreator):
    """
    Represents a comparison of the data in `col_name` with two levels:

    - Exact match in `col_name`
    - Anything else

    Args:
        col_name (str): The name of the column to compare
    """

    def __init__(
        self,
        col_name: str,
    ):
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class LevenshteinAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: Union[Iterable[int], int] = [1, 2],
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Levenshtein levels at specified distance thresholds
        - ...
        - Anything else

        For example, with distance_threshold_or_thresholds = [1, 3] the levels are

        - Exact match in `col_name`
        - Levenshtein distance in `col_name` <= 1
        - Levenshtein distance in `col_name` <= 3
        - Anything else

        Args:
            col_name (str): The name of the column to compare
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the levenshtein similarity level(s).
                Defaults to [1, 2].
        """

        thresholds_as_iterable: Iterable[int] = ensure_is_iterable(
            distance_threshold_or_thresholds
        )
        # unpack it to a list so we can repeat iteration if needed
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.LevenshteinLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class DamerauLevenshteinAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        distance_threshold_or_thresholds: Union[Iterable[int], int] = [1, 2],
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Damerau-Levenshtein levels at specified distance thresholds
        - ...
        - Anything else

        For example, with distance_threshold_or_thresholds = [1, 3] the levels are

        - Exact match in `col_name`
        - Damerau-Levenshtein distance in `col_name` <= 1
        - Damerau-Levenshtein distance in `col_name` <= 3
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            distance_threshold_or_thresholds (Union[int, list], optional): The
                threshold(s) to use for the Damerau-Levenshtein similarity level(s).
                Defaults to [1, 2].
        """

        thresholds_as_iterable = ensure_is_iterable(distance_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.DamerauLevenshteinLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"Damerau-Levenshtein distance at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class JaccardAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        score_threshold_or_thresholds: Union[Iterable[float], float] = [0.9, 0.7],
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Jaccard score levels at specified thresholds
        - ...
        - Anything else

        For example, with score_threshold_or_thresholds = [0.9, 0.7] the levels are:

        - Exact match in `col_name`
        - Jaccard score in `col_name` >= 0.9
        - Jaccard score in `col_name` >= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            score_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the Jaccard similarity level(s).
                Defaults to [0.9, 0.7].
        """

        thresholds_as_iterable = ensure_is_iterable(score_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.JaccardLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class JaroAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        score_threshold_or_thresholds: Union[Iterable[float], float] = [0.9, 0.7],
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Jaro score levels at specified thresholds
        - ...
        - Anything else

        For example, with score_threshold_or_thresholds = [0.9, 0.7] the levels are:

        - Exact match in `col_name`
        - Jaro score in `col_name` >= 0.9
        - Jaro score in `col_name` >= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            score_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the Jaro similarity level(s).
                Defaults to [0.9, 0.7].
        """

        thresholds_as_iterable = ensure_is_iterable(score_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.JaroLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class JaroWinklerAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        score_threshold_or_thresholds: Union[Iterable[float], float] = [0.9, 0.7],
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Jaro-Winkler score levels at specified thresholds
        - ...
        - Anything else

        For example, with score_threshold_or_thresholds = [0.9, 0.7] the levels are:

        - Exact match in `col_name`
        - Jaro-Winkler score in `col_name` >= 0.9
        - Jaro-Winkler score in `col_name` >= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            score_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the Jaro-Winkler similarity level(s).
                Defaults to [0.9, 0.7].
        """

        thresholds_as_iterable = ensure_is_iterable(score_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.JaroWinklerLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class DistanceFunctionAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        distance_function_name: str,
        distance_threshold_or_thresholds: Union[Iterable[float], float],
        higher_is_more_similar: bool = True,
    ):
        """
        Represents a comparison of the data in `col_name` with three or more levels:

        - Exact match in `col_name`
        - Custom distance function levels at specified thresholds
        - ...
        - Anything else

        For example, with distance_threshold_or_thresholds = [1, 3]
        and distance_function 'hamming', with higher_is_more_similar False
        the levels are:

        - Exact match in `col_name`
        - Hamming distance of `col_name` <= 1
        - Hamming distance of `col_name` <= 3
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            distance_function_name (str): the name of the SQL distance function
            distance_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the distance function level(s).
            higher_is_more_similar (bool): Are higher values of the distance function
                more similar? (e.g. True for Jaro-Winkler, False for Levenshtein)
                Default is True
        """

        thresholds_as_iterable = ensure_is_iterable(distance_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        self.distance_function_name = distance_function_name
        self.higher_is_more_similar = higher_is_more_similar
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            cll.ExactMatchLevel(self.col_expression),
            *[
                cll.DistanceFunctionLevel(
                    self.col_expression,
                    self.distance_function_name,
                    threshold,
                    higher_is_more_similar=self.higher_is_more_similar,
                )
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class PairwiseStringDistanceFunctionAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        distance_function_name: Literal[
            "levenshtein", "damerau_levenshtein", "jaro_winkler", "jaro"
        ],
        distance_threshold_or_thresholds: Union[Iterable[int | float], int | float],
    ):
        """
        Represents a comparison of the *most similar pair* of values
        where the first value is in the array data in `col_name` for the first record
        and the second value is in the array data in `col_name` for the second record.
        The comparison has three or more levels:

        - Exact match between any pair of values
        - User-selected string distance function levels at specified thresholds
        - ...
        - Anything else

        For example, with distance_threshold_or_thresholds = [1, 3]
        and distance_function 'levenshtein' the levels are:

        - Exact match between any pair of values
        - Levenshtein distance between the most similar pair of values <= 1
        - Levenshtein distance between the most similar pair of values <= 3
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            distance_function_name (str): the name of the string distance function.
                Must be one of "levenshtein," "damera_levenshtein," "jaro_winkler,"
                or "jaro."
            distance_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the distance function level(s).
        """
        thresholds_as_iterable = ensure_is_iterable(distance_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        self.distance_function_name = distance_function_name
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            # It is assumed that any string distance treats identical
            # arrays as the most similar
            cll.ArrayIntersectLevel(self.col_expression, min_intersection=1),
            *[
                cll.PairwiseStringDistanceFunctionLevel(
                    self.col_expression,
                    distance_threshold=threshold,
                    distance_function_name=self.distance_function_name,
                )
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        plural = "s" if len(self.thresholds) > 1 else ""
        return (
            f"Pairwise {self.distance_function_name} distance at threshold{plural} "
            f"{comma_separated_thresholds_string} vs. anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class AbsoluteTimeDifferenceAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        *,
        input_is_string: bool,
        metrics: Union[DateMetricType, List[DateMetricType]],
        thresholds: Union[int, float, List[Union[int, float]]],
        datetime_format: str = None,
        term_frequency_adjustments: bool = False,
        invalid_dates_as_null: bool = True,
    ):
        """
        Represents a comparison of the data in `col_name` with multiple levels based on
        absolute time differences:

        - Exact match in `col_name`
        - Absolute time difference levels at specified thresholds
        - ...
        - Anything else

        For example, with metrics = ['day', 'month'] and thresholds = [1, 3] the levels
        are:

        - Exact match in `col_name`
        - Absolute time difference in `col_name` <= 1 day
        - Absolute time difference in `col_name` <= 3 months
        - Anything else

        This comparison uses the AbsoluteTimeDifferenceLevel, which computes the total
        elapsed time between two dates, rather than counting calendar intervals.

        Args:
            col_name (str): The name of the column to compare.
            input_is_string (bool): If True, the input dates are treated as strings
                and parsed according to `datetime_format`.
            metrics (Union[DateMetricType, List[DateMetricType]]): The unit(s) of time
                to use when comparing dates. Can be 'second', 'minute', 'hour', 'day',
                'month', or 'year'.
            thresholds (Union[int, float, List[Union[int, float]]]): The threshold(s)
                to use for the time difference level(s).
            datetime_format (str, optional): The format string for parsing dates if
                `input_is_string` is True. ISO 8601 format used if not provided.
            term_frequency_adjustments (bool, optional): Whether to apply term frequency
                adjustments. Defaults to False.
            invalid_dates_as_null (bool, optional): If True and `input_is_string` is
                True, treat invalid dates as null. Defaults to True.
        """
        time_metrics_as_iterable = ensure_is_iterable(metrics)
        # unpack it to a list so we can repeat iteration if needed
        self.time_metrics = [*time_metrics_as_iterable]

        time_thresholds_as_iterable = ensure_is_iterable(thresholds)
        self.time_thresholds = [*time_thresholds_as_iterable]

        num_metrics = len(self.time_metrics)
        num_thresholds = len(self.time_thresholds)

        if num_thresholds == 0:
            raise ValueError("`time_thresholds` must have at least one entry")
        if num_metrics == 0:
            raise ValueError("`time_metrics` must have at least one entry")
        if num_metrics != num_thresholds:
            raise ValueError(
                "`time_thresholds` and `time_metrics` must have "
                "the same number of entries"
            )

        self.input_is_string = input_is_string

        if input_is_string:
            self.invalid_dates_as_null = invalid_dates_as_null
        else:
            self.invalid_dates_as_null = False

        self.datetime_format = datetime_format

        self.term_frequency_adjustments = term_frequency_adjustments

        super().__init__(col_name)

    @property
    def datetime_parse_function(self):
        return self.col_expression.try_parse_timestamp

    @property
    def cll_class(self):
        return cll.AbsoluteTimeDifferenceLevel

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        col = self.col_expression
        if self.invalid_dates_as_null:
            null_col = self.datetime_parse_function(self.datetime_format)
        else:
            null_col = col

        return [
            cll.NullLevel(null_col),
            cll.ExactMatchLevel(
                self.col_expression,
                term_frequency_adjustments=self.term_frequency_adjustments,
            ),
            *[
                self.cll_class(
                    col,
                    input_is_string=self.input_is_string,
                    threshold=time_threshold,
                    metric=time_metric,
                    datetime_format=self.datetime_format,
                )
                for (time_threshold, time_metric) in zip(
                    self.time_thresholds, self.time_metrics
                )
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class AbsoluteDateDifferenceAtThresholds(AbsoluteTimeDifferenceAtThresholds):
    @property
    def datetime_parse_function(self):
        return self.col_expression.try_parse_date

    @property
    def cll_class(self):
        return cll.AbsoluteDateDifferenceLevel


class ArrayIntersectAtSizes(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        size_threshold_or_thresholds: Union[Iterable[int], int] = [1],
    ):
        """
        Represents a comparison of the data in `col_name` with multiple levels based on
        the intersection sizes of array elements:

        - Intersection at specified size thresholds
        - ...
        - Anything else

        For example, with size_threshold_or_thresholds = [3, 1], the levels are:

        - Intersection of arrays in `col_name` has at least 3 elements
        - Intersection of arrays in `col_name` has at least 1 element
        - Anything else (e.g., empty intersection)

        Args:
            col_name (str): The name of the column to compare.
            size_threshold_or_thresholds (Union[int, list[int]], optional): The
                size threshold(s) for the intersection levels.
                Defaults to [1].
        """

        thresholds_as_iterable = ensure_is_iterable(size_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            *[
                cll.ArrayIntersectLevel(self.col_expression, min_intersection=threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class DistanceInKMAtThresholds(ComparisonCreator):
    def __init__(
        self,
        lat_col: str,
        long_col: str,
        km_thresholds: Union[Iterable[float], float],
    ):
        """
        A comparison of the latitude, longitude coordinates defined in
        'lat_col' and 'long col' giving the great circle distance between them in km.

        An example of the output with km_thresholds = [1, 10] would be:

        - The two coordinates are within 1 km of one another
        - The two coordinates are within 10 km of one another
        - Anything else (i.e. the distance between coordinates are > 10km apart)

        Args:
            lat_col (str): The name of the latitude column to compare.
            long_col (str): The name of the longitude column to compare.
            km_thresholds (iterable[float] | float): The km threshold(s) for the
                distance levels.
        """

        thresholds_as_iterable = ensure_is_iterable(km_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(
            col_name_or_names={
                "latitude_column": lat_col,
                "longitude_column": long_col,
            }
        )

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        lat_col = self.col_expressions["latitude_column"]
        long_col = self.col_expressions["longitude_column"]
        return [
            cll.Or(cll.NullLevel(lat_col), cll.NullLevel(long_col)),
            *[
                cll.DistanceInKMLevel(lat_col, long_col, km_threshold=threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        lat_col = self.col_expressions["latitude_column"]
        long_col = self.col_expressions["longitude_column"]
        return f"{lat_col.output_column_name}_{long_col.output_column_name}"


class CustomComparison(ComparisonCreator):
    def __init__(
        self,
        comparison_levels: List[Union[ComparisonLevelCreator, dict[str, Any]]],
        output_column_name: str = None,
        comparison_description: str = None,
    ):
        """
        Represents a comparison of the data with custom supplied levels.

        Args:
            output_column_name (str): The column name to use to refer to this comparison
            comparison_levels (list): A list of some combination of
                `ComparisonLevelCreator` objects, or dicts. These represent the
                similarity levels assessed by the comparison, in order of decreasing
                specificity
            comparison_description (str, optional): An optional description of the
                comparison
        """

        self._output_column_name = output_column_name
        self._comparison_levels = comparison_levels
        self._description = comparison_description
        # we deliberately don't call super().__init__() - all that does is set up
        # column expressions, which we do not need here as we are dealing with
        # levels directly

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        comparison_level_creators = [
            CustomLevel._convert_to_creator(cl) for cl in self._comparison_levels
        ]
        return comparison_level_creators

    def create_output_column_name(self) -> Optional[str]:
        # TODO: should default logic be here? would need column-extraction logic also
        return self._output_column_name

    @staticmethod
    def _convert_to_creator(
        comparison_creator: dict[str, Any] | ComparisonCreator,
    ) -> ComparisonCreator:
        if isinstance(comparison_creator, dict):
            return CustomComparison(**comparison_creator)
        return comparison_creator


class _DamerauLevenshteinIfSupportedElseLevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name: Union[str, ColumnExpression], distance_threshold: int):
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)
        self.distance_threshold = distance_threshold

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        try:
            lev_fn = sql_dialect.damerau_levenshtein_function_name
        except NotImplementedError:
            lev_fn = sql_dialect.levenshtein_function_name
        return f"{lev_fn}({col.name_l}, {col.name_r}) <= {self.distance_threshold}"

    def create_label_for_charts(self) -> str:
        col = self.col_expression
        return f"Levenshtein distance of {col.label} <= {self.distance_threshold}"


class DateOfBirthComparison(ComparisonCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        input_is_string: bool,
        datetime_thresholds: Union[int, float, List[Union[int, float]]] = [1, 1, 10],
        datetime_metrics: Union[DateMetricType, List[DateMetricType]] = [
            "month",
            "year",
            "year",
        ],
        datetime_format: str = None,
        invalid_dates_as_null: bool = True,
    ):
        """
        Generate an 'out of the box' comparison for a date of birth column
        in the `col_name` provided.

        Note that `input_is_string` is a required argument: you must denote whether the
        `col_name` contains if of type date/dattime or string.

        The default arguments will give a comparison with comparison levels:

        - Exact match (all other dates)
        - Damerau-Levenshtein distance <= 1
        - Date difference <= 1 month
        - Date difference <= 1 year
        - Date difference <= 10 years
        - Anything else

        Args:
            col_name (Union[str, ColumnExpression]): The column name
            input_is_string (bool): If True, the provided `col_name` must be of type
                string.  If False, it must be a date or datetime.
            datetime_thresholds (Union[int, float, List[Union[int, float]]], optional):
                Numeric thresholds for date differences. Defaults to [1, 1, 10].
            datetime_metrics (Union[DateMetricType, List[DateMetricType]], optional):
                Metrics for date differences. Defaults to ["month", "year", "year"].
            datetime_format (str, optional): The datetime format used to cast strings
                to dates.  Only used if input is a string.
            invalid_dates_as_null (bool, optional): If True, treat invalid dates as null
                as opposed to allowing e.g. an exact or levenshtein match where one side
                or both are an invalid date.  Only used if input is a string.  Defaults
                to True.
        """
        date_thresholds_as_iterable = ensure_is_iterable(datetime_thresholds)
        self.datetime_thresholds = [*date_thresholds_as_iterable]
        date_metrics_as_iterable = ensure_is_iterable(datetime_metrics)
        self.datetime_metrics = [*date_metrics_as_iterable]

        num_metrics = len(self.datetime_metrics)
        num_thresholds = len(self.datetime_thresholds)
        if num_thresholds == 0:
            raise ValueError("`date_thresholds` must have at least one entry")
        if num_metrics == 0:
            raise ValueError("`date_metrics` must have at least one entry")
        if num_metrics != num_thresholds:
            raise ValueError(
                "`date_thresholds` and `date_metrics` must have "
                "the same number of entries"
            )

        self.datetime_format = datetime_format

        self.input_is_string = input_is_string
        self.invalid_dates_as_null = invalid_dates_as_null

        super().__init__(col_name)

    @property
    def datetime_parse_function(self):
        return self.col_expression.try_parse_date

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        if self.invalid_dates_as_null and self.input_is_string:
            null_col = self.datetime_parse_function(self.datetime_format)
        else:
            null_col = self.col_expression

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(null_col),
        ]

        levels.append(
            cll.ExactMatchLevel(self.col_expression).configure(
                label_for_charts="Exact match on date of birth"
            )
        )

        if self.input_is_string:
            col_expr_as_string = self.col_expression
        else:
            col_expr_as_string = self.col_expression.cast_to_string()

        levels.append(
            _DamerauLevenshteinIfSupportedElseLevenshteinLevel(
                col_expr_as_string, distance_threshold=1
            ).configure(label_for_charts="DamerauLevenshtein distance <= 1")
        )

        if self.datetime_thresholds:
            for threshold, metric in zip(
                self.datetime_thresholds, self.datetime_metrics
            ):
                levels.append(
                    cll.AbsoluteDateDifferenceLevel(
                        self.col_expression,
                        threshold=threshold,
                        metric=metric,
                        input_is_string=self.input_is_string,
                    ).configure(
                        label_for_charts=f"Abs date difference <= {threshold} {metric}"
                    )
                )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class PostcodeComparison(ComparisonCreator):
    SECTOR_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9]"
    DISTRICT_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]?"
    AREA_REGEX = "^[A-Za-z]{1,2}"
    VALID_POSTCODE_REGEX = "^[A-Za-z]{1,2}[0-9][A-Za-z0-9]? [0-9][A-Za-z]{2}$"

    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        invalid_postcodes_as_null: bool = False,
        lat_col: Union[str, ColumnExpression] = None,
        long_col: Union[str, ColumnExpression] = None,
        km_thresholds: Union[float, List[float]] = [1, 10, 100],
    ):
        """
        Generate an 'out of the box' comparison for a postcode column with the
        in the `col_name` provided.

        The default comparison levels are:

        - Null comparison
        - Exact match on full postcode
        - Exact match on sector
        - Exact match on district
        - Exact match on area
        - Distance in km (if lat_col and long_col are provided)

        It's also possible to include levels for distance in km, but this requires
        you to have geocoded your postcodes prior to importing them into Splink. Use
        the `lat_col` and `long_col` arguments to tell Splink where to find the
        latitude and longitude columns.

        See https://ideal-postcodes.co.uk/guides/uk-postcode-format
        for definitions

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the postcodes to be compared.
            invalid_postcodes_as_null (bool, optional): If True, treat invalid postcodes
                as null. Defaults to False.
            lat_col (Union[str, ColumnExpression], optional): The column name or
                expression for latitude. Required if `km_thresholds` is provided.
            long_col (Union[str, ColumnExpression], optional): The column name or
                expression for longitude. Required if `km_thresholds` is provided.
            km_thresholds (Union[float, List[float]], optional): Thresholds for distance
                in kilometers. If provided, `lat_col` and `long_col` must also be
                provided.
        """
        self.valid_postcode_regex = (
            self.VALID_POSTCODE_REGEX if invalid_postcodes_as_null else None
        )

        cols = {"postcode": col_name}

        km_thresholds_as_iterable = ensure_is_iterable(km_thresholds)
        self.km_thresholds = [*km_thresholds_as_iterable]
        if lat_col is None or long_col is None:
            self.km_thresholds = []
        else:
            cols["latitude"] = lat_col
            cols["longitude"] = long_col

        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expressions["postcode"]
        sector_col_expression = full_col_expression.regex_extract(self.SECTOR_REGEX)
        district_col_expression = full_col_expression.regex_extract(self.DISTRICT_REGEX)
        area_col_expression = full_col_expression.regex_extract(self.AREA_REGEX)

        levels: list[ComparisonLevelCreator] = []

        if len(self.km_thresholds) == 0:
            levels = [
                cll.NullLevel(
                    full_col_expression, valid_string_pattern=self.valid_postcode_regex
                ),
                cll.ExactMatchLevel(full_col_expression).configure(
                    label_for_charts="Exact match on full postcode"
                ),
                cll.ExactMatchLevel(sector_col_expression).configure(
                    label_for_charts="Exact match on sector"
                ),
                cll.ExactMatchLevel(district_col_expression).configure(
                    label_for_charts="Exact match on district"
                ),
                cll.ExactMatchLevel(area_col_expression).configure(
                    label_for_charts="Exact match on area"
                ),
            ]
        if len(self.km_thresholds) > 0:
            # Don't include the very high level postcode categories
            # if using km thresholds - they are better modelled as geo distances
            levels = [
                cll.NullLevel(
                    full_col_expression, valid_string_pattern=self.valid_postcode_regex
                ),
                cll.ExactMatchLevel(full_col_expression),
                cll.ExactMatchLevel(sector_col_expression),
            ]

            lat_col_expression = self.col_expressions["latitude"]
            long_col_expression = self.col_expressions["longitude"]
            for km_threshold in self.km_thresholds:
                levels.append(
                    cll.DistanceInKMLevel(
                        lat_col_expression, long_col_expression, km_threshold
                    ).configure(label_for_charts=f"Distance in km <= {km_threshold}")
                )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expressions["postcode"].output_column_name


class EmailComparison(ComparisonCreator):
    USERNAME_REGEX = "^[^@]+"
    DOMAIN_REGEX = "@([^@]+)$"

    def __init__(self, col_name: Union[str, ColumnExpression]):
        """
        Generate an 'out of the box' comparison for an email address column with the
        in the `col_name` provided.

        The default comparison levels are:

        - Null comparison: e.g., one email is missing or invalid.
        - Exact match on full email: e.g., `john@smith.com` vs. `john@smith.com`.
        - Exact match on username part of email: e.g., `john@company.com` vs.
        `john@other.com`.
        - Jaro-Winkler similarity > 0.88 on full email: e.g., `john.smith@company.com`
        vs. `john.smyth@company.com`.
        - Jaro-Winkler similarity > 0.88 on username part of email: e.g.,
        `john.smith@company.com` vs. `john.smyth@other.com`.
        - Anything else: e.g., `john@company.com` vs. `rebecca@other.com`.

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the email addresses to be compared.
        """
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        full_col_expression = self.col_expression
        username_col_expression = full_col_expression.regex_extract(self.USERNAME_REGEX)

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(full_col_expression, valid_string_pattern=None),
            cll.ExactMatchLevel(full_col_expression).configure(
                tf_adjustment_column=full_col_expression.raw_sql_expression
            ),
            cll.ExactMatchLevel(username_col_expression).configure(
                label_for_charts="Exact match on username"
            ),
            cll.JaroWinklerLevel(full_col_expression, 0.88),
            cll.JaroWinklerLevel(username_col_expression, 0.88).configure(
                label_for_charts="Jaro-Winkler >0.88 on username"
            ),
            cll.ElseLevel(),
        ]
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


class NameComparison(ComparisonCreator):
    def __init__(
        self,
        col_name: Union[str, ColumnExpression],
        *,
        jaro_winkler_thresholds: Union[float, list[float]] = [0.92, 0.88, 0.7],
        dmeta_col_name: str = None,
    ):
        """
        Generate an 'out of the box' comparison for a name column in the `col_name`
        provided.

        It's also possible to include a level for a dmetaphone match, but this requires
        you to derive a dmetaphone column prior to importing it into Splink. Note
        this is expected to be a column containing arrays of dmetaphone values, which
        are of length 1 or 2.

        The default comparison levels are:

        - Null comparison
        - Exact match
        - Jaro-Winkler similarity > 0.92
        - Jaro-Winkler similarity > 0.88
        - Jaro-Winkler similarity > 0.70
        - Anything else

        Args:
            col_name (Union[str, ColumnExpression]): The column name or expression for
                the names to be compared.
            jaro_winkler_thresholds (Union[float, list[float]], optional): Thresholds
                for Jaro-Winkler similarity. Defaults to [0.92, 0.88, 0.7].
            dmeta_col_name (str, optional): The column name for dmetaphone values.
                If provided, array intersection level is included. This column must
                contain arrays of dmetaphone values, which are of length 1 or 2.
        """

        jaro_winkler_thresholds_itr = ensure_is_iterable(jaro_winkler_thresholds)
        self.jaro_winkler_thresholds = list(jaro_winkler_thresholds_itr)

        cols = {"name": col_name}
        if dmeta_col_name is not None:
            cols["dmeta_name"] = dmeta_col_name
            self.dmeta_col = True
        else:
            self.dmeta_col = False

        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        name_col_expression = self.col_expressions["name"]

        levels: list[ComparisonLevelCreator] = [
            cll.NullLevel(name_col_expression),
        ]

        exact_match = cll.ExactMatchLevel(name_col_expression)

        # Term frequencies can only be applied to 'pure' columns
        if name_col_expression.is_pure_column_or_column_reference:
            exact_match.configure(
                tf_adjustment_column=name_col_expression.raw_sql_expression
            )
        levels.append(exact_match)

        for threshold in self.jaro_winkler_thresholds:
            if threshold >= 0.88:
                levels.append(cll.JaroWinklerLevel(name_col_expression, threshold))

        if self.dmeta_col:
            dmeta_col_expression = self.col_expressions["dmeta_name"]
            levels.append(
                cll.ArrayIntersectLevel(dmeta_col_expression, min_intersection=1)
            )

        for threshold in self.jaro_winkler_thresholds:
            if threshold < 0.88:
                levels.append(cll.JaroWinklerLevel(name_col_expression, threshold))

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        return self.col_expressions["name"].output_column_name


class ForenameSurnameComparison(ComparisonCreator):
    def __init__(
        self,
        forename_col_name: Union[str, ColumnExpression],
        surname_col_name: Union[str, ColumnExpression],
        *,
        jaro_winkler_thresholds: Union[float, list[float]] = [0.92, 0.88],
        forename_surname_concat_col_name: str = None,
    ):
        """
        Generate an 'out of the box' comparison for forename and surname columns
        in the `forename_col_name` and `surname_col_name` provided.

        It's recommended to derive an additional column containing a concatenated
        forename and surname column so that term frequencies can be applied to the
        full name.  If you have derived a column, provide it at
        `forename_surname_concat_col_name`.

        The default comparison levels are:

        - Null comparison on both forename and surname
        - Exact match on both forename and surname
        - Columns reversed comparison (forename and surname swapped)
        - Jaro-Winkler similarity > 0.92 on both forename and surname
        - Jaro-Winkler similarity > 0.88 on both forename and surname
        - Exact match on surname
        - Exact match on forename
        - Anything else

        Args:
            forename_col_name (Union[str, ColumnExpression]): The column name or
                expression for the forenames to be compared.
            surname_col_name (Union[str, ColumnExpression]): The column name or
                expression for the surnames to be compared.
            jaro_winkler_thresholds (Union[float, list[float]], optional): Thresholds
                for Jaro-Winkler similarity. Defaults to [0.92, 0.88].
            forename_surname_concat_col_name (str, optional): The column name for
                concatenated forename and surname values. If provided, term
                frequencies are applied on the exact match using this column
        """
        jaro_winkler_thresholds_itr = ensure_is_iterable(jaro_winkler_thresholds)
        self.jaro_winkler_thresholds = list(jaro_winkler_thresholds_itr)
        cols = {"forename": forename_col_name, "surname": surname_col_name}
        if forename_surname_concat_col_name is not None:
            cols["forename_surname_concat"] = forename_surname_concat_col_name
        super().__init__(cols)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        forename_col_expression = self.col_expressions["forename"]
        surname_col_expression = self.col_expressions["surname"]

        levels: list[ComparisonLevelCreator] = [
            cll.And(
                cll.NullLevel(forename_col_expression),
                cll.NullLevel(surname_col_expression),
            )
        ]
        if "forename_surname_concat" in self.col_expressions:
            concat_col_expr = self.col_expressions["forename_surname_concat"]
            levels.append(
                cll.ExactMatchLevel(concat_col_expr).configure(
                    tf_adjustment_column=concat_col_expr.raw_sql_expression
                )
            )
        else:
            levels.append(
                cll.And(
                    cll.ExactMatchLevel(forename_col_expression),
                    cll.ExactMatchLevel(surname_col_expression),
                )
            )

        levels.append(
            cll.ColumnsReversedLevel(
                forename_col_expression, surname_col_expression, symmetrical=True
            )
        )

        for threshold in self.jaro_winkler_thresholds:
            levels.append(
                cll.And(
                    cll.JaroWinklerLevel(forename_col_expression, threshold),
                    cll.JaroWinklerLevel(surname_col_expression, threshold),
                )
            )

        levels.append(
            cll.ExactMatchLevel(surname_col_expression).configure(
                tf_adjustment_column=surname_col_expression.raw_sql_expression
            )
        )
        levels.append(
            cll.ExactMatchLevel(forename_col_expression).configure(
                tf_adjustment_column=forename_col_expression.raw_sql_expression
            )
        )

        levels.append(cll.ElseLevel())
        return levels

    def create_output_column_name(self) -> str:
        forename_output_name = self.col_expressions["forename"].output_column_name
        surname_output_name = self.col_expressions["surname"].output_column_name
        return f"{forename_output_name}_{surname_output_name}"


class CosineSimilarityAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        score_threshold_or_thresholds: Union[Iterable[float], float] = [0.9, 0.8, 0.7],
    ):
        """
        Represents a comparison of the data in `col_name` with two or more levels:

        - Cosine similarity levels at specified thresholds
        - ...
        - Anything else

        For example, with score_threshold_or_thresholds = [0.9, 0.7] the levels are:

        - Cosine similarity in `col_name` >= 0.9
        - Cosine similarity in `col_name` >= 0.7
        - Anything else

        Args:
            col_name (str): The name of the column to compare.
            score_threshold_or_thresholds (Union[float, list], optional): The
                threshold(s) to use for the cosine similarity level(s).
                Defaults to [0.9, 0.7].
        """

        thresholds_as_iterable = ensure_is_iterable(score_threshold_or_thresholds)
        self.thresholds = [*thresholds_as_iterable]
        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_expression),
            *[
                cll.CosineSimilarityLevel(self.col_expression, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name

from __future__ import annotations

from copy import copy
from typing import Any, Iterable, List, Optional, Union

from . import comparison_level_library as cll
from .comparison_creator import ComparisonCreator
from .comparison_level_creator import ComparisonLevelCreator
from .comparison_level_library import CustomLevel, DateMetricType
from .misc import ensure_is_iterable


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
            output_col_name (str): The column name to use to refer to this comparison
            comparison_levels (list): A list of some combination of
                `ComparisonLevelCreator` objects, or dicts. These represent the
                similarity levels assessed by the comparison, in order of decreasing
                specificity
            description (str, optional): An optional description of the comparison
        """

        self._output_column_name = output_column_name
        self._comparison_levels = comparison_levels
        self._description = comparison_description
        # we deliberately don't call super().__init__() - all that does is set up
        # column expressions, which we do not need here as we are dealing with
        # levels directly

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

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        comparison_level_creators = [
            self._convert_to_creator(cl) for cl in self._comparison_levels
        ]
        return comparison_level_creators

    def create_description(self) -> str:
        # TODO: fleshed out default description?
        return (
            self._description
            if self._description is not None
            else f"Comparison for {self._output_column_name}"
        )

    def create_output_column_name(self) -> Optional[str]:
        # TODO: should default logic be here? would need column-extraction logic also
        return self._output_column_name


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

    def create_description(self) -> str:
        return f"Exact match '{self.col_expression.label}' vs. anything else"

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

        thresholds_as_iterable = ensure_is_iterable(distance_threshold_or_thresholds)
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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"Levenshtein distance at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"Jaccard score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"Jaro score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"Jaro-Winkler score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"`{self.distance_function_name}` at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
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
        """Invalid dates as null does nothing if input is not a string"""
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

    def create_description(self) -> str:
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"abs time difference at thresholds "
            f"{self.time_thresholds} "
            f"with metrics {self.time_metrics} vs. "
            "anything else"
        )

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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        plural = "s" if len(self.thresholds) > 1 else ""
        return (
            f"Array intersection at minimum size{plural} "
            f"{comma_separated_thresholds_string} vs. anything else"
        )

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

        * The two coordinates are within 1 km of one another
        * The two coordinates are within 10 km of one another
        * Anything else (i.e. the distance between coordinates are > 10km apart)

        Args:
            lat_col(str): The name of the latitude column to compare.
            long_col(str): The name of the longitude column to compare.
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

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        plural = "s" if len(self.thresholds) > 1 else ""
        return (
            f"Distance in km at distance{plural} "
            f"{comma_separated_thresholds_string} vs. anything else"
        )

    def create_output_column_name(self) -> str:
        lat_col = self.col_expressions["latitude_column"]
        long_col = self.col_expressions["longitude_column"]
        return f"{lat_col.output_column_name}_{long_col.output_column_name}"

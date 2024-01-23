from typing import Iterable, List, Union

from . import comparison_level_library as cll
from .comparison_creator import ComparisonCreator
from .comparison_level_creator import ComparisonLevelCreator
from .comparison_level_library import CustomLevel
from .misc import ensure_is_iterable


class CustomComparison(ComparisonCreator):
    def __init__(
        self,
        output_column_name: str,
        comparison_levels: List[Union[ComparisonLevelCreator, dict]],
        description: str = None,
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
        self._description = description
        # we deliberately don't call super().__init__() - all that does is set up
        # column expressions, which we do not need here as we are dealing with
        # levels directly

    @staticmethod
    def _convert_to_creator(cl: Union[ComparisonLevelCreator, dict]):
        if isinstance(cl, ComparisonLevelCreator):
            return cl
        if isinstance(cl, dict):
            # TODO: swap this if we develop a more uniform approach to (de)serialising
            return CustomLevel(**cl)
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

    def create_output_column_name(self) -> str:
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
        distance_function_name,
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


class DatediffAtThresholds(ComparisonCreator):
    def __init__(
        self,
        col_name: str,
        *,
        date_metrics: Union[str, List[str]],
        date_thresholds: Union[int, List[int]],
        cast_strings_to_dates: bool = False,
        date_format: str = None,
        term_frequency_adjustments=False,
        invalid_dates_as_null=True,
    ):
        date_metrics_as_iterable = ensure_is_iterable(date_metrics)
        # unpack it to a list so we can repeat iteration if needed
        self.date_metrics = [*date_metrics_as_iterable]

        date_thresholds_as_iterable = ensure_is_iterable(date_thresholds)
        self.date_thresholds = [*date_thresholds_as_iterable]

        num_metrics = len(self.date_metrics)
        num_thresholds = len(self.date_thresholds)
        if num_thresholds == 0:
            raise ValueError("`date_thresholds` must have at least one entry")
        if num_metrics == 0:
            raise ValueError("`date_metrics` must have at least one entry")
        if num_metrics != num_thresholds:
            raise ValueError(
                "`date_thresholds` and `date_metrics` must have "
                "the same number of entries"
            )

        self.cast_strings_to_dates = cast_strings_to_dates
        self.date_format = date_format

        self.term_frequency_adjustments = term_frequency_adjustments

        self.invalid_dates_as_null = invalid_dates_as_null

        super().__init__(col_name)

    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        col = self.col_expression
        if self.invalid_dates_as_null:
            null_col = col.try_parse_date(self.date_format)
        else:
            null_col = col

        if self.cast_strings_to_dates:
            date_diff_col = col.try_parse_date(self.date_format)
        else:
            date_diff_col = col

        return [
            cll.NullLevel(null_col),
            cll.ExactMatchLevel(
                self.col_expression,
                term_frequency_adjustments=self.term_frequency_adjustments,
            ),
            *[
                cll.DatediffLevel(
                    date_diff_col,
                    date_threshold=date_threshold,
                    date_metric=date_metric,
                )
                for (date_threshold, date_metric) in zip(
                    self.date_thresholds, self.date_metrics
                )
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        return (
            f"Exact match '{self.col_expression.label}' vs. "
            f"date difference at thresholds "
            f"{self.date_thresholds} "
            f"with metrics {self.date_metrics} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_expression.output_column_name


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

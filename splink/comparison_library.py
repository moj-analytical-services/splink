from typing import Iterable, List, Union

from . import comparison_level_library as cll
from .comparison_creator import ComparisonCreator
from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect
from .misc import ensure_is_iterable


class ExactMatch(ComparisonCreator):
    """
    Represents a comparison of the data in `col_name` with two levels:
        - Exact match in `col_name`
        - Anything else

    Args:
        col_name (str): The name of the column to compare
    """

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        return f"Exact match '{self.col_name}' vs. anything else"

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            *[
                cll.LevenshteinLevel(self.col_name, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_name}' vs. "
            f"Levenshtein distance at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            *[
                cll.DamerauLevenshteinLevel(self.col_name, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_name}' vs. "
            f"Damerau-Levenshtein distance at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            *[
                cll.JaccardLevel(self.col_name, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_name}' vs. "
            f"Jaccard score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            *[cll.JaroLevel(self.col_name, threshold) for threshold in self.thresholds],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_name}' vs. "
            f"Jaro score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            *[
                cll.JaroWinklerLevel(self.col_name, threshold)
                for threshold in self.thresholds
            ],
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        comma_separated_thresholds_string = ", ".join(map(str, self.thresholds))
        return (
            f"Exact match '{self.col_name}' vs. "
            f"Jaro-Winkler score at thresholds "
            f"{comma_separated_thresholds_string} vs. "
            "anything else"
        )

    def create_output_column_name(self) -> str:
        return self.col_name


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

    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            *[
                cll.ArrayIntersectLevel(self.col_name, min_intersection=threshold)
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
        return self.col_name

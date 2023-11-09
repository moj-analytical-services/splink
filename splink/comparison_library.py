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

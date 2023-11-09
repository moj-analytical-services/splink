from typing import List

from . import comparison_level_library as cll
from .comparison_creator import ComparisonCreator
from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect


class ExactMatch(ComparisonCreator):
    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        return [
            cll.NullLevel(self.col_name),
            cll.ExactMatchLevel(self.col_name),
            cll.ElseLevel(),
        ]

    def create_description(self) -> str:
        return "Cool description"

    def create_output_column_name(self) -> str:
        return self.col_name

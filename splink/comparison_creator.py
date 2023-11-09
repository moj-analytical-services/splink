from abc import ABC, abstractmethod
from typing import List, final

from .comparison import Comparison
from .comparison_level_creator import ComparisonLevelCreator
from .dialects import SplinkDialect
from .input_column import InputColumn


class ComparisonCreator(ABC):
    # TODO: need to think about what this is used for - do we need multiple columns
    # if we are sticking with storing a col_name ?
    def __init__(self, col_name: str = None):
        """
        Class to author Comparisons
        Args:
            col_name (str): Input column name
        """
        self.col_name = col_name

    @abstractmethod
    def create_comparison_levels(
        self, sql_dialect: SplinkDialect
    ) -> List[ComparisonLevelCreator]:
        pass

    @abstractmethod
    def create_description(self) -> str:
        pass

    @abstractmethod
    def create_output_column_name(self) -> str:
        pass

    @final
    def get_comparison(self, sql_dialect_str: str) -> Comparison:
        """sql_dialect_str is a string to make this method easier to use
        for the end user - otherwise they'd need to import a SplinkDialect"""
        return Comparison(self.create_comparison_dict(sql_dialect_str))

    @final
    def create_comparison_dict(self, sql_dialect_str: str) -> dict:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict = {
            "comparison_description": self.create_description(),
            "output_column_name": self.create_output_column_name(),
            "comparison_levels": [
                cl.get_comparison_level(sql_dialect_str)
                for cl in self.create_comparison_levels(sql_dialect)
            ],
        }

        return level_dict

    @final
    def input_column(self, sql_dialect: SplinkDialect) -> InputColumn:
        return InputColumn(self.col_name, sql_dialect=sql_dialect.sqlglot_name)

    def __repr__(self) -> str:
        return (
            f"Comparison generator for {self.create_description()}. "
            "Call .get_comparison(sql_dialect) to instantiate "
            "a Comparison"
        )

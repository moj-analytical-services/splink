from abc import ABC, abstractmethod
from typing import List, Union, final

from .column_expression import ColumnExpression
from .comparison import Comparison
from .comparison_level_creator import ComparisonLevelCreator


class ComparisonCreator(ABC):
    # TODO: need to think about what this is used for - do we need multiple columns
    # if we are sticking with storing a col_name ?
    def __init__(self, col_name: Union[str, ColumnExpression] = None):
        """
        Class to author Comparisons
        Args:
            col_name (str): Input column name
        """
        self.col_expression = ColumnExpression.instantiate_if_str(col_name)

    # TODO: property?
    @abstractmethod
    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        pass

    @final
    def get_configured_comparison_levels(self) -> List[ComparisonLevelCreator]:
        # furnish comparison levels with m and u probabilities as needed
        comparison_levels = self.create_comparison_levels()

        if self.term_frequency_adjustments:
            for cl in comparison_levels:
                if cl.is_exact_match_level:
                    cl.term_frequency_adjustments = True

        if self.m_probabilities:
            m_values = self.m_probabilities.copy()
            comparison_levels = [
                cl.configure(
                    m_probability=m_values.pop(0) if not cl.is_null_level else None,
                )
                for cl in comparison_levels
            ]
        if self.u_probabilities:
            u_values = self.u_probabilities.copy()
            comparison_levels = [
                cl.configure(
                    u_probability=u_values.pop(0) if not cl.is_null_level else None,
                )
                for cl in comparison_levels
            ]
        return comparison_levels

    @final
    @property
    def num_levels(self) -> int:
        return len(self.create_comparison_levels())

    @final
    @property
    def num_non_null_levels(self) -> int:
        return len(
            [cl for cl in self.create_comparison_levels() if not cl.is_null_level]
        )

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
        level_dict = {
            "comparison_description": self.create_description(),
            "output_column_name": self.create_output_column_name(),
            "comparison_levels": [
                cl.get_comparison_level(sql_dialect_str)
                for cl in self.get_configured_comparison_levels()
            ],
        }

        return level_dict

    @final
    def configure(
        self,
        *,
        term_frequency_adjustments: bool = False,
        m_probabilities: list[float] = None,
        u_probabilities: list[float] = None,
    ) -> "ComparisonCreator":
        """
        Configure the comparison creator with m and u probabilities. The first
        element in the list corresponds to the first comparison level, usually
        an exact match level. Subsequent elements correspond comparison to
        levels in sequential order, through to the last element which is usually
        the 'ELSE' level.

        Example:
            cc = LevenshteinAtThresholds("name", 2)
            cc.configure(
                m_probabilities=[0.9, 0.08, 0.02],
                u_probabilities=[0.01, 0.05, 0.94]
                # probabilities for exact match level, levenshtein <= 2, and else
                # in that order
            )
        Args:
            term_frequency_adjustments (bool, optional): Whether term frequency
                adjustments are switched on for this comparison. Only applied
                to exact match levels. Default: False
            m_probabilities (list, optional): List of m probabilities
            u_probabilities (list, optional): List of u probabilities
        """
        self.term_frequency_adjustments = term_frequency_adjustments
        self.m_probabilities = m_probabilities
        self.u_probabilities = u_probabilities
        return self

    @property
    def term_frequency_adjustments(self):
        return getattr(self, "_term_frequency_adjustments", False)

    @final
    @term_frequency_adjustments.setter
    def term_frequency_adjustments(self, term_frequency_adjustments: bool):
        self._term_frequency_adjustments = term_frequency_adjustments

    @final
    @property
    def m_probabilities(self):
        return getattr(self, "_m_probabilities", None)

    @final
    @m_probabilities.setter
    def m_probabilities(self, m_probabilities: list[float]):
        if m_probabilities:
            num_probs_supplied = len(m_probabilities)
            num_non_null_levels = self.num_non_null_levels
            if num_probs_supplied != self.num_non_null_levels:
                raise ValueError(
                    f"Comparison has {num_non_null_levels} non-null levels, "
                    f"but received {num_probs_supplied} values for m_probabilities. "
                    "These numbers must be the same."
                )
            self._m_probabilities = m_probabilities

    @final
    @property
    def u_probabilities(self):
        return getattr(self, "_u_probabilities", None)

    @final
    @u_probabilities.setter
    def u_probabilities(self, u_probabilities: list[float]):
        if u_probabilities:
            num_probs_supplied = len(u_probabilities)
            num_non_null_levels = self.num_non_null_levels
            if num_probs_supplied != self.num_non_null_levels:
                raise ValueError(
                    f"Comparison has {num_non_null_levels} non-null levels, "
                    f"but received {num_probs_supplied} values for u_probabilities. "
                    "These numbers must be the same."
                )
            self._u_probabilities = u_probabilities

    def __repr__(self) -> str:
        return (
            f"Comparison generator for {self.create_description()}. "
            "Call .get_comparison(sql_dialect_str) to instantiate "
            "a Comparison"
        )

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, final

from splink.internals.column_expression import ColumnExpression
from splink.internals.exceptions import SplinkException

from .comparison import Comparison
from .comparison_level_creator import (
    ComparisonLevelCreator,
    UnsuppliedNoneOr,
    unsupplied_option,
)


class ComparisonCreator(ABC):
    DEFAULT_COL_EXP_KEY = "__default__"

    def __init__(
        self,
        col_name_or_names: Union[
            Dict[str, Union[str, ColumnExpression]], Union[str, ColumnExpression]
        ],
    ):
        """
        Class to author Comparisons
        Args:
            col_name_or_names (str, ColumnExpression): Input column name(s).
                Can be a single item or a dict.
        """
        # if it's not a dict, assume it is a single expression-like
        if not isinstance(col_name_or_names, dict):
            cols = {self.DEFAULT_COL_EXP_KEY: col_name_or_names}
        else:
            cols = col_name_or_names

        self.col_expressions = {
            name_reference: ColumnExpression.instantiate_if_str(column)
            for name_reference, column in cols.items()
        }
        self._validate()

    # many ComparisonCreators have a single column expression, so provide a
    # convenience property for this case. Error if there are none or many
    @property
    def col_expression(self) -> ColumnExpression:
        num_cols = len(self.col_expressions)
        if num_cols > 1:
            raise SplinkException(
                "Cannot get `ComparisonLevelCreator.col_expression` when "
                f"`.col_expressions` has more than one element: {type(self)}"
            )
        if num_cols == 0:
            raise SplinkException(
                "Cannot get `ComparisonLevelCreator.col_expression` when "
                f"`.col_expressions` has no elements: {type(self)}"
            )
        try:
            col_expression = self.col_expressions[self.DEFAULT_COL_EXP_KEY]
        except KeyError:
            raise SplinkException(
                "Cannot get `ComparisonLevelCreator.col_expression` when "
                f"`.col_expressions` has non-default single entry: {type(self)}"
            ) from None
        return col_expression

    def _validate(self) -> None:
        # create levels - let them raise errors if there are issues
        self.create_comparison_levels()

    @abstractmethod
    def create_comparison_levels(self) -> List[ComparisonLevelCreator]:
        pass

    @final
    def get_configured_comparison_levels(self) -> List[ComparisonLevelCreator]:
        # furnish comparison levels with m and u probabilities as needed
        comparison_levels = self.create_comparison_levels()

        if self.term_frequency_adjustments:
            for cl in comparison_levels:
                if (
                    hasattr(cl, "col_expression")
                    and cl.col_expression.is_pure_column_or_column_reference
                    and cl.is_exact_match_level
                ):
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

    def create_description(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def create_output_column_name(self) -> Optional[str]:
        # should be str where possible, otherwise a default will be created
        pass

    @final
    def get_comparison(self, sql_dialect_str: str) -> Comparison:
        """sql_dialect_str is a string to make this method easier to use
        for the end user - otherwise they'd need to import a SplinkDialect"""
        return Comparison(
            **self.create_comparison_dict(sql_dialect_str),
            sqlglot_dialect=sql_dialect_str,
        )

    @final
    def create_comparison_dict(self, sql_dialect_str: str) -> dict[str, Any]:
        level_dict = {
            "comparison_description": self.create_description(),
            "output_column_name": self.create_output_column_name(),
            "comparison_levels": [
                cl.get_comparison_level(sql_dialect_str).as_dict()
                for cl in self.get_configured_comparison_levels()
            ],
        }
        return level_dict

    @final
    def configure(
        self,
        *,
        term_frequency_adjustments: UnsuppliedNoneOr[bool] = unsupplied_option,
        m_probabilities: UnsuppliedNoneOr[List[float]] = unsupplied_option,
        u_probabilities: UnsuppliedNoneOr[List[float]] = unsupplied_option,
    ) -> "ComparisonCreator":
        """
        Configure the comparison creator with options that are common to all
        comparisons.

        For m and u probabilities, the first
        element in the list corresponds to the first comparison level, usually
        an exact match level. Subsequent elements correspond comparison to
        levels in sequential order, through to the last element which is usually
        the 'ELSE' level.

        All options have default options set initially. Any call to `.configure()`
        will set any options that are supplied. Any subsequent calls to `.configure()`
        will not override these values with defaults; to override values you must
        explicitly provide a value corresponding to the default.

        Generally speaking only a single call (at most) to `.configure()` should
        be required.

        Args:
            term_frequency_adjustments (bool, optional): Whether term frequency
                adjustments are switched on for this comparison. Only applied
                to exact match levels.
                Default corresponds to False.
            m_probabilities (list, optional): List of m probabilities
                Default corresponds to None.
            u_probabilities (list, optional): List of u probabilities
                Default corresponds to None.

        Example:
            ```py
            cc = LevenshteinAtThresholds("name", 2)
            cc.configure(
                m_probabilities=[0.9, 0.08, 0.02],
                u_probabilities=[0.01, 0.05, 0.94]
                # probabilities for exact match level, levenshtein <= 2, and else
                # in that order
            )
            ```

        """
        configurables = {
            "term_frequency_adjustments": term_frequency_adjustments,
            "m_probabilities": m_probabilities,
            "u_probabilities": u_probabilities,
        }

        for attribute_name, attribute_value in configurables.items():
            if attribute_value is not unsupplied_option:
                setattr(self, attribute_name, attribute_value)

        return self

    @property
    def term_frequency_adjustments(self):
        return getattr(self, "_term_frequency_adjustments", False)

    @term_frequency_adjustments.setter
    @final
    def term_frequency_adjustments(self, term_frequency_adjustments: bool) -> None:
        self._term_frequency_adjustments = term_frequency_adjustments

    @property
    def m_probabilities(self):
        return getattr(self, "_m_probabilities", None)

    @m_probabilities.setter
    @final
    def m_probabilities(self, m_probabilities: List[float]) -> None:
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

    @property
    def u_probabilities(self):
        return getattr(self, "_u_probabilities", None)

    @u_probabilities.setter
    @final
    def u_probabilities(self, u_probabilities: List[float]) -> None:
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

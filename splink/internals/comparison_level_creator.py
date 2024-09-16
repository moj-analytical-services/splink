from __future__ import annotations

from abc import ABC, abstractmethod
from inspect import signature
from typing import Any, TypeVar, Union, final

from splink.internals.column_expression import ColumnExpression
from splink.internals.dialects import SplinkDialect

from .comparison_level import ComparisonLevel


class _UnsuppliedOption:
    _instance: "_UnsuppliedOption" | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_UnsuppliedOption, cls).__new__(cls)
        return cls._instance


unsupplied_option = _UnsuppliedOption()

T = TypeVar("T")
# type alias - either the specified type, _UnsuppliedOption, or None
UnsuppliedNoneOr = Union[T, _UnsuppliedOption, None]


class ComparisonLevelCreator(ABC):
    # off by default - only a small subset should have tf adjustments
    term_frequency_adjustments = False

    @abstractmethod
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        pass

    @abstractmethod
    def create_label_for_charts(self) -> str:
        pass

    @final
    def get_comparison_level(self, sql_dialect_str: str) -> ComparisonLevel:
        """sql_dialect_str is a string to make this method easier to use
        for the end user - otherwise they'd need to import a SplinkDialect"""
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        return ComparisonLevel(
            sqlglot_dialect=sql_dialect.sqlglot_dialect,
            **self.create_level_dict(sql_dialect_str),
        )

    @final
    def create_level_dict(self, sql_dialect_str: str) -> dict[str, Any]:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict = {
            "sql_condition": self.create_sql(sql_dialect),
            "label_for_charts": self.create_label_for_charts(),
        }

        # additional config options get passed only if created via .configure()
        allowed_attrs = [s for s in signature(self.configure).parameters if s != "self"]

        for attr in allowed_attrs:
            if (value := getattr(self, attr, None)) is not None:
                if isinstance(value, ColumnExpression):
                    value.sql_dialect = sql_dialect
                    value = value.name
                level_dict[attr] = value

        return level_dict

    @final
    def configure(
        self,
        *,
        m_probability: UnsuppliedNoneOr[float] = unsupplied_option,
        u_probability: UnsuppliedNoneOr[float] = unsupplied_option,
        tf_adjustment_column: UnsuppliedNoneOr[str] = unsupplied_option,
        tf_adjustment_weight: UnsuppliedNoneOr[float] = unsupplied_option,
        tf_minimum_u_value: UnsuppliedNoneOr[float] = unsupplied_option,
        is_null_level: UnsuppliedNoneOr[bool] = unsupplied_option,
        label_for_charts: UnsuppliedNoneOr[str] = unsupplied_option,
        disable_tf_exact_match_detection: UnsuppliedNoneOr[bool] = unsupplied_option,
        fix_m_probability: UnsuppliedNoneOr[bool] = unsupplied_option,
        fix_u_probability: UnsuppliedNoneOr[bool] = unsupplied_option,
    ) -> "ComparisonLevelCreator":
        """
        Configure the comparison level with options which are common to all
        comparison levels.  The options align to the keys in the json
        specification of a comparison level.  These options are usually not
        needed, but are available for advanced users.

        All options have default options set initially. Any call to `.configure()`
        will set any options that are supplied. Any subsequent calls to `.configure()`
        will not override these values with defaults; to override values you must must
        explicitly provide a value corresponding to the default.

        Generally speaking only a single call (at most) to `.configure()` should
        be required.

        Args:
            m_probability (float, optional): The m probability for this
                comparison level.
                Default is equivalent to None, in which case a default initial value
                will be provided for this level.
            u_probability (float, optional): The u probability for this
                comparison level.
                Default is equivalent to None, in which case a default initial value
                will be provided for this level.
            tf_adjustment_column (str, optional): Make term frequency adjustments for
                this comparison level using this input column.
                Default is equivalent to None, meaning that term-frequency adjustments
                will not be applied for this level.
            tf_adjustment_weight (float, optional): Make term frequency adjustments
                for this comparison level using this weight.
                Default is equivalent to None, meaning term-frequency adjustments are
                fully-weighted if turned on.
            tf_minimum_u_value (float, optional): When term frequency adjustments are
                turned on, where the term frequency adjustment implies a u value below
                this value, use this minimum value instead.
                Defaults is equivalent to None, meaning no minimum value.
            is_null_level (bool, optional): If true, m and u values will not be
                estimated and instead the match weight will be zero for this column.
                Default is equivalent to False.
            label_for_charts (str, optional): If provided, a custom label that will
                be used for this level in any charts.
                Default is equivalent to None, in which case a default label will be
                provided for this level.
            disable_tf_exact_match_detection (bool, optional): If true, if term
                frequency adjustments are set, the corresponding adjustment will be
                made using the u-value for _this_ level, rather than the usual case
                where it is the u-value of the exact match level in the same comparison.
                Default is equivalent to False.
            fix_m_probability (bool, optional): If true, the m probability for this
                level will be fixed and not estimated during training.
                Default is equivalent to False.
            fix_u_probability (bool, optional): If true, the u probability for this
                level will be fixed and not estimated during training.
                Default is equivalent to False.
        Returns:
            ComparisonLevelCreator: The instance of the ComparisonLevelCreator class
                with the updated configuration.
        """
        args = locals()
        del args["self"]
        for k, v in args.items():
            if v is not unsupplied_option:
                setattr(self, k, v)

        return self

    @property
    def is_null_level(self) -> bool:
        return getattr(self, "_is_null_level", False)

    @is_null_level.setter
    @final
    def is_null_level(self, is_null_level: bool) -> None:
        self._is_null_level = is_null_level

    @property
    def is_exact_match_level(self) -> bool:
        return getattr(self, "_is_exact_match_level", False)

    @is_exact_match_level.setter
    @final
    def is_exact_match_level(self, is_exact_match_level: bool) -> None:
        self._is_exact_match_level = is_exact_match_level

    def __repr__(self) -> str:
        return (
            f"Comparison level generator for {self.create_label_for_charts()}. "
            "Call .get_comparison_level(sql_dialect_str) to instantiate "
            "a ComparisonLevel"
        )

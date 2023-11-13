from abc import ABC, abstractmethod
from inspect import signature
from typing import final

from .comparison_level import ComparisonLevel
from .dialects import SplinkDialect
from .input_column import InputColumn


class ComparisonLevelCreator(ABC):
    def __init__(self, col_name: str = None):
        """
        Class to author ComparisonLevels
        Args:
            col_name (str): Input column name
        """
        self.col_name = col_name

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
        return ComparisonLevel(self.create_level_dict(sql_dialect_str))

    @final
    def create_level_dict(self, sql_dialect_str: str) -> dict:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict = {
            "sql_condition": self.create_sql(sql_dialect),
            "label_for_charts": self.create_label_for_charts(),
        }

        # additional config options get passed only if created via .configure()
        allowed_attrs = [s for s in signature(self.configure).parameters if s != "self"]

        for attr in allowed_attrs:
            if (value := getattr(self, attr, None)) is not None:
                level_dict[attr] = value

        return level_dict

    @final
    def input_column(self, sql_dialect: SplinkDialect) -> InputColumn:
        return InputColumn(self.col_name, sql_dialect=sql_dialect.sqlglot_name)

    @final
    def configure(
        self,
        *,
        m_probability: float = None,
        u_probability: float = None,
        tf_adjustment_column: str = None,
        tf_adjustment_weight: float = None,
        tf_minimum_u_value: float = None,
        is_null_level: bool = None,
    ) -> "ComparisonLevelCreator":
        """
        Configure the comparison level with options which are common to all
        comparison levels.  The options align to the keys in the json
        specification of a comparison level.  These options are usually not
        needed, but are available for advanced users.


        Args:
            m_probability (float, optional): The m probability for this
                comparison level. Defaults to None, meaning it is not set.
            u_probability (float, optional): The u probability for this
                comparison level. Defaults to None, meaning it is not set.
            tf_adjustment_column (str, optional): Make term frequency adjustments for
                this comparison level using this input column. Defaults to None,
                meaning term-frequency adjustments will not be applied for this level.
            tf_adjustment_weight (float, optional): Make term frequency adjustments
                for this comparison level using this weight. Defaults to None,
                meaning term-frequency adjustments are fully-weighted if turned on.
            tf_minimum_u_value (float, optional): When term frequency adjustments are
                turned on, where the term frequency adjustment implies a u value below
                this value, use this minimum value instead. Defaults to None, meaning
                no minimum value.
            is_null_level (bool, optional): If true, m and u values will not be
                estimated and instead the match weight will be zero for this column.
                Defaults to None, equivalent to False.

        Returns:
            ComparisonLevelCreator: The instance of the ComparisonLevelCreator class
                with the updated configuration.
        """
        args = locals()
        del args["self"]
        for k, v in args.items():
            if v is not None:
                setattr(self, k, v)

        return self

    def __repr__(self) -> str:
        return (
            f"Comparison level generator for {self.create_label_for_charts()}. "
            "Call .get_comparison_level(sql_dialect_str) to instantiate "
            "a ComparisonLevel"
        )

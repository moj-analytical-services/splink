from abc import ABC, abstractmethod
from inspect import signature
from typing import final

from .comparison_level import ComparisonLevel
from .dialects import SplinkDialect
from .input_column import InputColumn


class ComparisonLevelCreator(ABC):
    def __init__(self, col_name: str = None):
        """_summary_

        Args:
            col_name (str): _description_
            dialect (str, optional): _description_. Defaults to None.
            m_probability (_type_, optional): _description_. Defaults to None.
        """
        self.col_name = col_name
        # TODO: do we need to set these to None, or let ComparisonLevel handle missings?
        self.m_probability = None
        self.u_probability = None
        self.tf_adjustment_column = None
        self.tf_adjustment_weight = None
        self.tf_minimum_u_value = None
        self.is_null_level = None

    @abstractmethod
    def create_sql(self, dialect: SplinkDialect):
        pass

    @abstractmethod
    def create_label_for_charts(self):
        pass

    @final
    def get_comparison_level(self, sql_dialect: str):
        return ComparisonLevel(self.create_level_dict(sql_dialect))

    @final
    def create_level_dict(self, sql_dialect: str):
        dialect = SplinkDialect.from_string(sql_dialect)
        level_dict = {
            "sql_condition": self.create_sql(dialect),
            "label_for_charts": self.create_label_for_charts(),
        }

        # additional config options get passed only if created via .configure()
        allowed_attrs = [s for s in signature(self.configure).parameters if s != "self"]

        for attr in allowed_attrs:
            if (value := getattr(self, attr, None)) is not None:
                level_dict[attr] = value

        return level_dict

    @final
    def input_column(self, sql_dialect: SplinkDialect):
        return InputColumn(self.col_name, sql_dialect=sql_dialect.name)

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
    ):
        """_summary_

        Args:
            m_probability (float, optional): _description_. Defaults to None.
            u_probability (float, optional): _description_. Defaults to None.
            tf_adjustment_column (str, optional): _description_. Defaults to None.
            tf_adjustment_weight (float, optional): _description_. Defaults to None.
            tf_minimum_u_value (float, optional): _description_. Defaults to None.
            is_null_level (bool, optional): _description_. Defaults to None.
        """
        args = locals()
        del args["self"]
        for k, v in args.items():
            if v is not None:
                setattr(self, k, v)

        return self

    def __repr__(self):
        return (
            f"Comparison level generator for {self.create_label_for_charts()}. "
            "Call .get_comparison_level(sql_dialect) to instantiate "
            "a ComparisonLevel"
        )

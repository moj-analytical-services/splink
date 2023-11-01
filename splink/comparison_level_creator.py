from abc import ABC, abstractmethod
from typing import final

from .comparison_level import ComparisonLevel
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
    def create_sql(self, sql_dialect: str):
        pass

    @abstractmethod
    def create_label_for_charts(self):
        pass

    @final
    def get_comparison_level(self, sql_dialect: str):
        return ComparisonLevel(self.create_level_dict(sql_dialect))

    @final
    def create_level_dict(self, sql_dialect: str):
        level_dict = {
            "sql_condition": self.create_sql(sql_dialect),
            "label_for_charts": self.create_label_for_charts(),
        }

        if self.m_probability:
            level_dict["m_probability"] = self.m_probability
        if self.u_probability:
            level_dict["u_probability"] = self.u_probability
        if self.tf_adjustment_column:
            level_dict["tf_adjustment_column"] = self.tf_adjustment_column
        if self.tf_adjustment_weight:
            level_dict["tf_adjustment_weight"] = self.tf_adjustment_weight
        if self.tf_minimum_u_value:
            level_dict["tf_minimum_u_value"] = self.tf_minimum_u_value
        if self.is_null_level:
            level_dict["is_null_level"] = self.is_null_level

        return level_dict

    @final
    def input_column(self, sql_dialect: str):
        return InputColumn(self.col_name, sql_dialect=sql_dialect)

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

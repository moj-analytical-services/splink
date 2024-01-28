from abc import ABC, abstractmethod
from typing import Union, final

from sqlglot import TokenError, parse_one

from .blocking import BlockingRule, blocking_rule_to_obj
from .column_expression import ColumnExpression
from .dialects import SplinkDialect


def _translate_sql_string(
    sqlglot_base_dialect_sql: str,
    to_sqlglot_dialect: str,
    from_sqlglot_dialect: str = None,
) -> str:
    tree = parse_one(sqlglot_base_dialect_sql, read=from_sqlglot_dialect)

    return tree.sql(dialect=to_sqlglot_dialect)


class BlockingRuleCreator(ABC):
    def __init__(self, salting_partitions=None, arrays_to_explode=None):
        self._salting_partitions = salting_partitions
        self._arrays_to_explode = arrays_to_explode

    # @property because merged levels need logic to determine salting partitions
    @property
    def salting_partitions(self):
        return self._salting_partitions

    @property
    def arrays_to_explode(self):
        return self._arrays_to_explode

    @abstractmethod
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        pass

    @final
    def create_blocking_rule_dict(self, sql_dialect_str: str) -> dict:
        sql_dialect = SplinkDialect.from_string(sql_dialect_str)
        level_dict = {
            "blocking_rule": self.create_sql(sql_dialect),
            "sql_dialect": sql_dialect_str,
        }

        if self.salting_partitions and self.arrays_to_explode:
            raise ValueError("Cannot use both salting_partitions and arrays_to_explode")

        if self.salting_partitions:
            level_dict["salting_partitions"] = self.salting_partitions

        if self.arrays_to_explode:
            level_dict["arrays_to_explode"] = self.arrays_to_explode

        return level_dict

    @final
    def get_blocking_rule(self, sql_dialect_str: str) -> BlockingRule:
        return blocking_rule_to_obj(self.create_blocking_rule_dict(sql_dialect_str))


class ExactMatchRule(BlockingRuleCreator):
    def __init__(
        self,
        col_name_or_expr: Union[str, ColumnExpression],
        salting_partitions=None,
        arrays_to_explode=None,
    ):
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        self.col_expression = ColumnExpression.instantiate_if_str(col_name_or_expr)

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        self.col_expression.sql_dialect = sql_dialect
        col = self.col_expression
        return f"{col.l_name} = {col.r_name}"


class CustomRule(BlockingRuleCreator):
    def __init__(
        self,
        sql_condition: str,
        base_dialect_str: str = None,
        salting_partitions=None,
        arrays_to_explode=None,
    ):
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        self.sql_condition = sql_condition

        self.base_dialect_str = base_dialect_str

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        sql_condition = self.sql_condition
        if self.base_dialect_str is not None:
            base_dialect = SplinkDialect.from_string(self.base_dialect_str)
            # if we are told it is one dialect, but try to create comparison level
            # of another, try to translate with sqlglot
            if sql_dialect != base_dialect:
                base_dialect_sqlglot_name = base_dialect.sqlglot_name

                # as default, translate condition into our dialect
                try:
                    sql_condition = _translate_sql_string(
                        sql_condition,
                        sql_dialect.sqlglot_name,
                        base_dialect_sqlglot_name,
                    )
                # if we hit a sqlglot error, assume users knows what they are doing,
                # e.g. it is something custom / unknown to sqlglot
                # error will just appear when they try to use it
                except TokenError:
                    pass
        return sql_condition


class _Merge(BlockingRuleCreator):
    @final
    def __init__(
        self,
        *blocking_rules: Union[BlockingRuleCreator, dict],
        salting_partitions=None,
        arrays_to_explode=None,
    ):
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        num_levels = len(blocking_rules)
        if num_levels == 0:
            raise ValueError(
                f"Must provide at least one blocking rule to {type(self)}()"
            )
        self.blocking_rules = blocking_rules

    @property
    def salting_partitions(self):
        if (
            hasattr(self, "_salting_partitions")
            and self._salting_partitions is not None
        ):
            return self._salting_partitions

        return max(
            [
                br.salting_partitions
                for br in self.blocking_rules
                if br.salting_partitions is not None
            ],
            default=None,
        )

    @property
    def arrays_to_explode(self):
        if hasattr(self, "_arrays_to_explode"):
            return self._arrays_to_explode

        if any([br.arrays_to_explode for br in self.blocking_rules]):
            raise ValueError("Cannot merge blocking rules with arrays_to_explode")
        return None

    @final
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f" {self._clause} ".join(
            map(lambda cl: f"({cl.create_sql(sql_dialect)})", self.blocking_rules)
        )


class And(_Merge):
    _clause = "AND"


class Or(_Merge):
    _clause = "OR"


class Not(BlockingRuleCreator):
    def __init__(self, blocking_rule_creator):
        self.blocking_rule_creator = blocking_rule_creator

    @property
    def salting_partitions(self):
        return self.blocking_rule_creator.salting_partitions

    @property
    def arrays_to_explode(self):
        if self.blocking_rule_creator.arrays_to_explode:
            raise ValueError("Cannot use arrays_to_explode with Not")
        return None

    @final
    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        return f"NOT ({self.blocking_rule_creator.create_sql(sql_dialect)})"


def block_on(
    *col_names_or_exprs: Union[str, ColumnExpression],
    salting_partitions=None,
    arrays_to_explode=None,
) -> BlockingRuleCreator:
    if len(col_names_or_exprs) == 1:
        br = ExactMatchRule(col_names_or_exprs[0])
    else:
        br = And(*[ExactMatchRule(c) for c in col_names_or_exprs])

    if salting_partitions:
        br._salting_partitions = salting_partitions
    if arrays_to_explode:
        br._arrays_to_explode = arrays_to_explode
    return br

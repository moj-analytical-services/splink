from __future__ import annotations

from typing import Any, Union, final

from sqlglot import TokenError, parse_one

from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.column_expression import ColumnExpression
from splink.internals.dialects import SplinkDialect


def _translate_sql_string(
    sqlglot_base_dialect_sql: str,
    to_sqlglot_dialect: str,
    from_sqlglot_dialect: str = None,
) -> str:
    tree = parse_one(sqlglot_base_dialect_sql, read=from_sqlglot_dialect)

    return tree.sql(dialect=to_sqlglot_dialect)


class ExactMatchRule(BlockingRuleCreator):
    def __init__(
        self,
        col_name_or_expr: Union[str, ColumnExpression],
        salting_partitions: int = None,
        arrays_to_explode: list[str] | None = None,
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
        blocking_rule: str,
        sql_dialect: str = None,
        salting_partitions: int | None = None,
        arrays_to_explode: list[str] | None = None,
    ):
        """
        Represents a custom blocking rule using a user-defined SQL condition.  To
        refer to the left hand side and the right hand side of the pairwise
        record comparison, use `l` and `r` respectively, e.g.
        `l.first_name = r.first_name and len(l.first_name) <2`.

        Args:
            blocking_rule (str): A SQL condition string representing the custom
                blocking rule.
            sql_dialect (str, optional): The SQL dialect of the provided blocking rule.
                If specified, Splink will attempt to translate the rule to the
                appropriate dialect.
            salting_partitions (int, optional): The number of partitions to use for
                salting. If provided, enables salting for this blocking rule.
            arrays_to_explode (list[str], optional): A list of array column names
                to explode before applying the blocking rule.

        Examples:
            ```python
            from splink.blocking_rule_library import CustomRule

            # Simple custom rule
            rule_1 = CustomRule("l.postcode = r.postcode")

            # Custom rule with dialect translation
            rule_2 = CustomRule(
                "SUBSTR(l.surname, 1, 3) = SUBSTR(r.surname, 1, 3)",
                sql_dialect="sqlite"
            )

            # Custom rule with salting
            rule_3 = CustomRule(
                "l.city = r.city",
                salting_partitions=10
            )
            ```
        """
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        self.sql_condition = blocking_rule

        self.base_dialect_str = sql_dialect

    def create_sql(self, sql_dialect: SplinkDialect) -> str:
        sql_condition = self.sql_condition
        if self.base_dialect_str is not None:
            base_dialect = SplinkDialect.from_string(self.base_dialect_str)
            # if we are told it is one dialect, but try to create comparison level
            # of another, try to translate with sqlglot
            if sql_dialect != base_dialect:
                base_dialect_sqlglot_name = base_dialect.sqlglot_dialect

                # as default, translate condition into our dialect
                try:
                    sql_condition = _translate_sql_string(
                        sql_condition,
                        sql_dialect.sqlglot_dialect,
                        base_dialect_sqlglot_name,
                    )
                # if we hit a sqlglot error, assume users knows what they are doing,
                # e.g. it is something custom / unknown to sqlglot
                # error will just appear when they try to use it
                except TokenError:
                    pass
        return sql_condition


class _Merge(BlockingRuleCreator):
    _clause = ""

    @final
    def __init__(
        self,
        *blocking_rules: Union[BlockingRuleCreator, dict[str, Any]],
        salting_partitions: int | None = None,
        arrays_to_explode: list[str] | None = None,
    ):
        super().__init__(
            salting_partitions=salting_partitions, arrays_to_explode=arrays_to_explode
        )
        num_levels = len(blocking_rules)
        if num_levels == 0:
            raise ValueError(
                f"Must provide at least one blocking rule to {type(self)}()"
            )
        blocking_rule_creators = [
            CustomRule(**br) if isinstance(br, dict) else br for br in blocking_rules
        ]
        self.blocking_rules = blocking_rule_creators

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
    salting_partitions: int | None = None,
    arrays_to_explode: list[str] | None = None,
) -> BlockingRuleCreator:
    """Generates blocking rules of equality conditions  based on the columns
    or SQL expressions specified.

    When multiple columns or SQL snippets are provided, the function generates a
    compound blocking rule, connecting individual match conditions with
    "AND" clauses.

    Further information on equi-join conditions can be found
    [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html)

    Args:
        col_names_or_exprs: A list of input columns or SQL conditions
            you wish to create blocks on.
        salting_partitions (optional, int): Whether to add salting
            to the blocking rule. More information on salting can
            be found within the docs.
        arrays_to_explode (optional, List[str]): List of arrays to explode
            before applying the blocking rule.

    Examples:
        ``` python
        from splink import block_on
        br_1 = block_on("first_name")
        br_2 = block_on("substr(surname,1,2)", "surname")
        ```

    """
    if isinstance(col_names_or_exprs[0], list):
        raise TypeError(
            "block_on no longer accepts a list as the first argument. "
            "Please pass individual column names or expressions as separate arguments"
            ' e.g. block_on("first_name", "dob") not block_on(["first_name", "dob"])'
        )

    if len(col_names_or_exprs) == 1:
        br: BlockingRuleCreator = ExactMatchRule(col_names_or_exprs[0])
    else:
        br = And(*[ExactMatchRule(c) for c in col_names_or_exprs])

    if salting_partitions:
        br._salting_partitions = salting_partitions
    if arrays_to_explode:
        br._arrays_to_explode = arrays_to_explode
    return br

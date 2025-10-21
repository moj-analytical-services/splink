from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Literal, Optional, TypedDict

from sqlglot import parse_one
from sqlglot.expressions import Column, Expression, Identifier, Join
from sqlglot.optimizer.eliminate_joins import join_condition
from sqlglot.optimizer.optimizer import optimize
from sqlglot.optimizer.simplify import flatten

from splink.internals.database_api import DatabaseAPISubClass
from splink.internals.dialects import SplinkDialect
from splink.internals.input_column import InputColumn
from splink.internals.misc import ensure_is_list
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.unique_id_concat import _composite_unique_id_from_nodes_sql
from splink.internals.vertically_concatenate import vertically_concatenate_sql

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from splink.internals.settings import LinkTypeLiteralType

user_input_link_type_options = Literal["link_only", "link_and_dedupe", "dedupe_only"]

backend_link_type_options = Literal[
    "link_only", "link_and_dedupe", "dedupe_only", "two_dataset_link_only", "self_link"
]


class BlockingRuleDict(TypedDict):
    blocking_rule: str
    sql_dialect: str
    salting_partitions: int | None
    arrays_to_explode: list[str] | None


def blocking_rule_to_obj(br: BlockingRule | BlockingRuleDict) -> BlockingRule:
    if isinstance(br, BlockingRule):
        return br
    elif isinstance(br, dict):
        blocking_rule = br.get("blocking_rule", None)
        if blocking_rule is None:
            raise ValueError("No blocking rule submitted...")
        sql_dialect_str = br.get("sql_dialect", None)
        if sql_dialect_str is None:
            raise ValueError("Must provide a valid sql_dialect")
        salting_partitions = br.get("salting_partitions", None)
        arrays_to_explode = br.get("arrays_to_explode", None)

        if arrays_to_explode is not None and salting_partitions is not None:
            raise ValueError(
                "Splink does not support blocking rules that are "
                " both salted and exploding"
            )

        if salting_partitions is not None:
            return SaltedBlockingRule(
                blocking_rule, sql_dialect_str, salting_partitions
            )

        if arrays_to_explode is not None:
            return ExplodingBlockingRule(
                blocking_rule, sql_dialect_str, arrays_to_explode
            )

        return BlockingRule(blocking_rule, sql_dialect_str)

    raise TypeError(f"'br' must be of type 'BlockingRule' or 'dict', not {type(br)}")


def combine_unique_id_input_columns(
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
) -> List[InputColumn]:
    unique_id_input_columns: List[InputColumn] = []
    if source_dataset_input_column:
        unique_id_input_columns.append(source_dataset_input_column)
    unique_id_input_columns.append(unique_id_input_column)
    return unique_id_input_columns


class BlockingRule:
    def __init__(
        self,
        blocking_rule_sql: str,
        sql_dialect_str: str,
    ):
        if sql_dialect_str is None:
            raise TypeError("BlockingRule requires a valid 'sql_dialect_str'")
        self._sql_dialect_str = sql_dialect_str

        # Temporarily just to see if tests still pass
        if not isinstance(blocking_rule_sql, str):
            raise ValueError(
                f"Blocking rule must be a string, not {type(blocking_rule_sql)}"
            )
        self.blocking_rule_sql = blocking_rule_sql
        self.preceding_rules: List[BlockingRule] = []

    @property
    def sqlglot_dialect(self):
        return SplinkDialect.from_string(self._sql_dialect_str).sqlglot_dialect

    @property
    def match_key(self):
        return len(self.preceding_rules)

    def add_preceding_rules(self, rules):
        rules = ensure_is_list(rules)
        self.preceding_rules = rules

    @staticmethod
    def _add_preceding_rules_to_each_blocking_rule(
        brs_as_objs: list[BlockingRule],
    ) -> list[BlockingRule]:
        for n, br in enumerate(brs_as_objs):
            br.add_preceding_rules(brs_as_objs[:n])
        return brs_as_objs

    def exclude_pairs_generated_by_this_rule_sql(
        self,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
    ) -> str:
        """A SQL string specifying how to exclude the results
        of THIS blocking rule from subseqent blocking statements,
        so that subsequent statements do not produce duplicate pairs
        """

        # Note the coalesce function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        return f"coalesce(({self.blocking_rule_sql}),false)"

    def exclude_pairs_generated_by_all_preceding_rules_sql(
        self,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
    ) -> str:
        """A SQL string that excludes the results of ALL previous blocking rules from
        the pairwise comparisons generated.
        """
        if not self.preceding_rules:
            return ""
        or_clauses = [
            br.exclude_pairs_generated_by_this_rule_sql(
                source_dataset_input_column,
                unique_id_input_column,
            )
            for br in self.preceding_rules
        ]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"

    def create_blocked_pairs_sql(
        self,
        *,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
        input_tablename_l: str,
        input_tablename_r: str,
        where_condition: str,
    ) -> str:
        if source_dataset_input_column:
            unique_id_columns = [source_dataset_input_column, unique_id_input_column]
        else:
            unique_id_columns = [unique_id_input_column]

        uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
        uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")

        sql = f"""
            select
            '{self.match_key}' as match_key,
            {uid_l_expr} as join_key_l,
            {uid_r_expr} as join_key_r
            from {input_tablename_l} as l
            inner join {input_tablename_r} as r
            on
            ({self.blocking_rule_sql})
            {where_condition}
            {self.exclude_pairs_generated_by_all_preceding_rules_sql(
                source_dataset_input_column,
                unique_id_input_column)
            }
            """
        return sql

    def create_blocking_input_sql(
        self,
        input_tablename: str,
        input_columns: List[InputColumn],
    ) -> str:
        """A SQL string that creates the input tables that will be joined
        for this blocking rule"""
        return f"select * from {input_tablename}"

    @property
    def _parsed_join_condition(self) -> Join:
        br = self.blocking_rule_sql
        br_flattened = flatten(parse_one(br, dialect=self.sqlglot_dialect)).sql(
            dialect=self.sqlglot_dialect
        )
        return parse_one("INNER JOIN r", into=Join).on(
            br_flattened, dialect=self.sqlglot_dialect
        )  # using sqlglot==11.4.1

    @property
    def _equi_join_conditions(self):
        """
        Extract the equi join conditions from the blocking rule as a tuple:
        source_keys, join_keys

        Returns:
            list of tuples like [(name, name), (substr(name,1,2), substr(name,2,3))]
        """

        def remove_table_prefix(tree: Expression) -> Expression:
            for c in tree.find_all(Column):
                del c.args["table"]
            return tree

        j: Join = self._parsed_join_condition

        source_keys, join_keys, _ = join_condition(j)

        keys_zipped = zip(source_keys, join_keys)

        rmtp = remove_table_prefix

        keys_de_prefixed: list[tuple[Expression, Expression]] = [
            (rmtp(i), rmtp(j)) for (i, j) in keys_zipped
        ]

        keys_strings: list[tuple[str, str]] = [
            (i.sql(dialect=self.sqlglot_dialect), j.sql(self.sqlglot_dialect))
            for (i, j) in keys_de_prefixed
        ]

        return keys_strings

    @property
    def _filter_conditions(self):
        # A more accurate term might be "non-equi-join conditions"
        # or "complex join conditions", but to capture the idea these are
        # filters that have to be applied post-creation of the pairwise record
        # comparison i've opted to call it a filter
        j = self._parsed_join_condition
        _, _, filter_condition = join_condition(j)
        if not filter_condition:
            return ""
        else:
            filter_condition = optimize(filter_condition)
            for i in filter_condition.find_all(Identifier):
                i.set("quoted", False)

            return filter_condition.sql(self.sqlglot_dialect)

    def as_dict(self):
        "The minimal representation of the blocking rule"
        output = {}

        output["blocking_rule"] = self.blocking_rule_sql
        output["sql_dialect"] = self._sql_dialect_str

        return output

    def _as_completed_dict(self):
        return self.blocking_rule_sql

    @property
    def descr(self):
        return "Custom" if not hasattr(self, "_description") else self._description

    def _abbreviated_sql(self, cutoff=75):
        sql = self.blocking_rule_sql
        return (sql[:cutoff] + "...") if len(sql) > cutoff else sql

    def __repr__(self):
        return f"<{self._human_readable_succinct}>"

    @property
    def _human_readable_succinct(self):
        sql = self._abbreviated_sql(75)
        return f"{self.descr} blocking rule using SQL: {sql}"


class SaltedBlockingRule(BlockingRule):
    def __init__(
        self,
        blocking_rule: str,
        sqlglot_dialect: str,
        salting_partitions: int = 1,
    ):
        if salting_partitions is None or salting_partitions <= 1:
            raise ValueError("Salting partitions must be specified and > 1")

        super().__init__(blocking_rule, sqlglot_dialect)
        self.salting_partitions = salting_partitions

    def as_dict(self):
        output = super().as_dict()
        output["salting_partitions"] = self.salting_partitions
        return output

    def _as_completed_dict(self):
        return self.as_dict()

    def _salting_condition(self, salt):
        return f"AND ceiling(l.__splink_salt * {self.salting_partitions}) = {salt + 1}"

    def create_blocked_pairs_sql(
        self,
        *,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
        input_tablename_l: str,
        input_tablename_r: str,
        where_condition: str,
    ) -> str:
        if source_dataset_input_column:
            unique_id_columns = [source_dataset_input_column, unique_id_input_column]
        else:
            unique_id_columns = [unique_id_input_column]

        uid_l_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "l")
        uid_r_expr = _composite_unique_id_from_nodes_sql(unique_id_columns, "r")

        sqls = []
        exclude_sql = self.exclude_pairs_generated_by_all_preceding_rules_sql(
            source_dataset_input_column, unique_id_input_column
        )
        for salt in range(self.salting_partitions):
            salt_condition = self._salting_condition(salt)
            sql = f"""
            select
            '{self.match_key}' as match_key,
            {uid_l_expr} as join_key_l,
            {uid_r_expr} as join_key_r
            from {input_tablename_l} as l
            inner join {input_tablename_r} as r
            on
            ({self.blocking_rule_sql} {salt_condition})
            {where_condition}
            {exclude_sql}
            """

            sqls.append(sql)
        return " UNION ALL ".join(sqls)


def _explode_arrays_sql(db_api, tbl_name, columns_to_explode, other_columns_to_retain):
    return db_api.sql_dialect.explode_arrays_sql(
        tbl_name, columns_to_explode, other_columns_to_retain
    )


class ExplodingBlockingRule(BlockingRule):
    def __init__(
        self,
        blocking_rule: BlockingRule | dict[str, Any] | str,
        sqlglot_dialect: str,
        array_columns_to_explode: list[str] = [],
    ):
        if isinstance(blocking_rule, BlockingRule):
            blocking_rule_sql = blocking_rule.blocking_rule_sql
        elif isinstance(blocking_rule, dict):
            blocking_rule_sql = blocking_rule["blocking_rule_sql"]
        else:
            blocking_rule_sql = blocking_rule
        super().__init__(blocking_rule_sql, sqlglot_dialect)
        self.array_columns_to_explode: List[str] = array_columns_to_explode
        self.exploded_id_pair_table: Optional[SplinkDataFrame] = None

    def marginal_exploded_id_pairs_table_sql(
        self,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
        br: BlockingRule,
        link_type: "LinkTypeLiteralType",
    ) -> str:
        """generates a table of the marginal id pairs from the exploded blocking rule
        i.e. pairs are only created that match this blocking rule and NOT any of
        the preceding blocking rules
        """

        unique_id_col = unique_id_input_column
        unique_id_input_columns = combine_unique_id_input_columns(
            source_dataset_input_column, unique_id_input_column
        )

        where_condition = _sql_gen_where_condition(link_type, unique_id_input_columns)

        id_expr_l = _composite_unique_id_from_nodes_sql(unique_id_input_columns, "l")
        id_expr_r = _composite_unique_id_from_nodes_sql(unique_id_input_columns, "r")

        if link_type == "two_dataset_link_only":
            where_condition = (
                where_condition + " and l.source_dataset < r.source_dataset"
            )

        exclude_sql = self.exclude_pairs_generated_by_all_preceding_rules_sql(
            source_dataset_input_column, unique_id_input_column
        )
        sql = f"""
            select distinct
                {id_expr_l} as {unique_id_col.name_l},
                {id_expr_r} as {unique_id_col.name_r}
            from __splink__df_concat_unnested as l
            inner join __splink__df_concat_unnested as r
            on ({br.blocking_rule_sql})
            {where_condition}
            {exclude_sql}
            """

        return sql

    def drop_materialised_id_pairs_dataframe(self):
        if self.exploded_id_pair_table is not None:
            self.exploded_id_pair_table.drop_table_from_database_and_remove_from_cache()
        self.exploded_id_pair_table = None

    def exclude_pairs_generated_by_this_rule_sql(
        self,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
    ) -> str:
        """A SQL string specifying how to exclude the results
        of THIS blocking rule from subseqent blocking statements,
        so that subsequent statements do not produce duplicate pairs
        """

        return "false"

    def create_blocked_pairs_sql(
        self,
        *,
        source_dataset_input_column: Optional[InputColumn],
        unique_id_input_column: InputColumn,
        input_tablename_l: str,
        input_tablename_r: str,
        where_condition: str,
    ) -> str:
        if self.exploded_id_pair_table is None:
            raise ValueError(
                "Exploding blocking rules are not supported for the function you have"
                " called."
            )

        exploded_id_pair_table = self.exploded_id_pair_table
        sql = f"""
            select
                '{self.match_key}' as match_key,
                {unique_id_input_column.name_l} as join_key_l,
                {unique_id_input_column.name_r} as join_key_r
            from {exploded_id_pair_table.physical_name}
        """
        return sql

    def create_blocking_input_sql(
        self,
        input_tablename: str,
        input_columns: List[InputColumn],
    ) -> str:
        """A SQL string that creates the input tables that will be joined
        for this blocking rule"""
        input_colnames = {col.quote().name for col in input_columns}

        arrays_to_explode_quoted = [
            InputColumn(colname, sqlglot_dialect_str=self.sqlglot_dialect).quote().name
            for colname in self.array_columns_to_explode
        ]

        dialect = SplinkDialect.from_string(self._sql_dialect_str)

        expl_sql = dialect.explode_arrays_sql(
            input_tablename,
            arrays_to_explode_quoted,
            list(input_colnames.difference(arrays_to_explode_quoted)),
        )

        return expl_sql

    def as_dict(self):
        output = super().as_dict()
        output["arrays_to_explode"] = self.array_columns_to_explode
        return output


def materialise_exploded_id_tables(
    link_type: "LinkTypeLiteralType",
    blocking_rules: List[BlockingRule],
    db_api: DatabaseAPISubClass,
    splink_df_dict: dict[str, SplinkDataFrame],
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
) -> list[ExplodingBlockingRule]:
    exploding_blocking_rules = [
        br for br in blocking_rules if isinstance(br, ExplodingBlockingRule)
    ]

    if len(exploding_blocking_rules) == 0:
        return []
    exploded_tables = []

    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_input_column=source_dataset_input_column,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")
    nodes_concat = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    input_colnames = {col.name for col in nodes_concat.columns}

    for br in exploding_blocking_rules:
        pipeline = CTEPipeline([nodes_concat])
        arrays_to_explode_quoted = [
            InputColumn(colname, sqlglot_dialect_str=db_api.sql_dialect.sqlglot_dialect)
            .quote()
            .name
            for colname in br.array_columns_to_explode
        ]

        expl_sql = db_api.sql_dialect.explode_arrays_sql(
            "__splink__df_concat",
            br.array_columns_to_explode,
            list(input_colnames.difference(arrays_to_explode_quoted)),
        )

        pipeline.enqueue_sql(
            expl_sql,
            "__splink__df_concat_unnested",
        )

        base_name = "__splink__marginal_exploded_ids_blocking_rule"
        table_name = f"{base_name}_mk_{br.match_key}"

        sql = br.marginal_exploded_id_pairs_table_sql(
            source_dataset_input_column=source_dataset_input_column,
            unique_id_input_column=unique_id_input_column,
            br=br,
            link_type=link_type,
        )

        pipeline.enqueue_sql(sql, table_name)

        marginal_ids_table = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        br.exploded_id_pair_table = marginal_ids_table
        exploded_tables.append(marginal_ids_table)

    return exploding_blocking_rules


def _sql_gen_where_condition(
    link_type: backend_link_type_options, unique_id_cols: List[InputColumn]
) -> str:
    id_expr_l = _composite_unique_id_from_nodes_sql(unique_id_cols, "l")
    id_expr_r = _composite_unique_id_from_nodes_sql(unique_id_cols, "r")

    if link_type in ("two_dataset_link_only", "self_link"):
        where_condition = " where 1=1 "
    elif link_type in ["link_and_dedupe", "dedupe_only"]:
        where_condition = f"where {id_expr_l} < {id_expr_r}"
    elif link_type == "link_only":
        source_dataset_col = unique_id_cols[0]
        where_condition = (
            f"where {id_expr_l} < {id_expr_r} "
            f"and l.{source_dataset_col.name} != r.{source_dataset_col.name}"
        )

    return where_condition


def block_using_rules_sqls(
    *,
    input_tablename_l: str,
    input_tablename_r: str,
    blocking_rules: List[BlockingRule],
    link_type: "LinkTypeLiteralType",
    source_dataset_input_column: Optional[InputColumn],
    unique_id_input_column: InputColumn,
) -> list[dict[str, str]]:
    """Use the blocking rules specified in the linker's settings object to
    generate a SQL statement that will create pairwise record comparions
    according to the blocking rule(s).

    Where there are multiple blocking rules, the SQL statement contains logic
    so that duplicate comparisons are not generated.
    """

    sqls = []

    unique_id_input_columns = combine_unique_id_input_columns(
        source_dataset_input_column, unique_id_input_column
    )

    where_condition = _sql_gen_where_condition(link_type, unique_id_input_columns)

    # Cover the case where there are no blocking rules
    # This is a bit of a hack where if you do a self-join on 'true'
    # you create a cartesian product, rather than having separate code
    # that generates a cross join for the case of no blocking rules
    if not blocking_rules:
        blocking_rules = [BlockingRule("1=1", sql_dialect_str="spark")]

    br_sqls = []

    for br in blocking_rules:
        sql = br.create_blocked_pairs_sql(
            unique_id_input_column=unique_id_input_column,
            source_dataset_input_column=source_dataset_input_column,
            input_tablename_l=input_tablename_l,
            input_tablename_r=input_tablename_r,
            where_condition=where_condition,
        )
        br_sqls.append(sql)

    sql = " UNION ALL ".join(br_sqls)

    if any(isinstance(br, ExplodingBlockingRule) for br in blocking_rules):
        sqls.append(
            {"sql": sql, "output_table_name": "__splink__blocked_id_pairs_non_unique"}
        )

        sql = """
        SELECT
            min(match_key) as match_key,
            join_key_l,
            join_key_r
        FROM __splink__blocked_id_pairs_non_unique
        GROUP BY join_key_l, join_key_r
        """

    sqls.append({"sql": sql, "output_table_name": "__splink__blocked_id_pairs"})

    return sqls

from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, List

from sqlglot import parse_one
from sqlglot.expressions import Column, Join
from sqlglot.optimizer.eliminate_joins import join_condition

from .input_column import InputColumn
from .misc import ensure_is_list
from .unique_id_concat import _composite_unique_id_from_nodes_sql
from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def blocking_rule_to_obj(br):
    if isinstance(br, BlockingRule):
        return br
    elif isinstance(br, dict):
        blocking_rule = br.get("blocking_rule", None)
        if blocking_rule is None:
            raise ValueError("No blocking rule submitted...")
        sqlglot_dialect = br.get("sql_dialect", None)
        salting_partitions = br.get("salting_partitions", 1)
        arrays_to_explode = br.get("arrays_to_explode", list())

        if arrays_to_explode:
            return ExplodingBlockingRule(
                blocking_rule, salting_partitions, sqlglot_dialect, arrays_to_explode
            )

        return BlockingRule(blocking_rule, salting_partitions, sqlglot_dialect)

    else:
        br = BlockingRule(br)
        return br


class BlockingRule:
    def __init__(
        self,
        blocking_rule: BlockingRule | dict | str,
        salting_partitions=1,
        sqlglot_dialect: str = None,
        array_columns_to_explode: list = [],
    ):
        if sqlglot_dialect:
            self._sql_dialect = sqlglot_dialect

        self.blocking_rule_sql = blocking_rule
        self.preceding_rules = []
        self.sqlglot_dialect = sqlglot_dialect
        self.salting_partitions: int = salting_partitions

    @property
    def sql_dialect(self):
        return None if not hasattr(self, "_sql_dialect") else self._sql_dialect

    @property
    def match_key(self):
        return len(self.preceding_rules)

    @property
    def is_salted(self):
        return self.salting_partitions > 1

    @property
    def sql(self):
        # Wrapper to reveal the underlying SQL
        return self.blocking_rule_sql

    def add_preceding_rules(self, rules):
        rules = ensure_is_list(rules)
        self.preceding_rules = rules

    def exclude_pairs_generated_by_this_rule_sql(self, linker: Linker):
        """A SQL string specifying how to exclude the results
        of THIS blocking rule from subseqent blocking statements,
        so that subsequent statements do not produce duplicate pairs
        """

        # Note the coalesce function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        return f"coalesce(({self.blocking_rule_sql}),false)"

    def exclude_pairs_generated_by_all_preceding_rules_sql(self, linker: Linker):
        """A SQL string that excludes the results of ALL previous blocking rules from
        the pairwise comparisons generated.
        """
        if not self.preceding_rules:
            return ""
        or_clauses = [
            br.exclude_pairs_generated_by_this_rule_sql(linker)
            for br in self.preceding_rules
        ]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"

    @property
    def salted_blocking_rules(self) -> List[SaltedBlockingRuleSegment]:
        """A list of sql strings"""

        for n in range(self.salting_partitions):
            if self.is_salted:
                rule_sql = (
                    f"{self.blocking_rule_sql} and "
                    f"ceiling(l.__splink_salt * {self.salting_partitions}) "
                    f"= {n+1}"
                )
            else:
                rule_sql = self.blocking_rule_sql

            br = SaltedBlockingRuleSegment(self, rule_sql, n)

            # Exploding blocking rules may have a materialised exploded_id_pair_table
            # If so, we want to associated it with the SaltedBlockingRuleSegment
            if isinstance(self, ExplodingBlockingRule):
                try:
                    br.exploded_id_pair_table = self.exploded_id_pair_tables[n]
                except IndexError:
                    pass

            yield br

    @property
    def _parsed_join_condition(self):
        br = self.blocking_rule_sql
        return parse_one("INNER JOIN r", into=Join).on(
            br, dialect=self.sqlglot_dialect
        )  # using sqlglot==11.4.1

    @property
    def _equi_join_conditions(self):
        """
        Extract the equi join conditions from the blocking rule as a tuple:
        source_keys, join_keys

        Returns:
            list of tuples like [(name, name), (substr(name,1,2), substr(name,2,3))]
        """

        def remove_table_prefix(tree):
            for c in tree.find_all(Column):
                del c.args["table"]
            return tree

        j = self._parsed_join_condition

        source_keys, join_keys, _ = join_condition(j)

        keys = zip(source_keys, join_keys)

        rmtp = remove_table_prefix

        keys = [(rmtp(i), rmtp(j)) for (i, j) in keys]

        keys = [
            (i.sql(dialect=self.sqlglot_dialect), j.sql(self.sqlglot_dialect))
            for (i, j) in keys
        ]

        return keys

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
            return filter_condition.sql(self.sqlglot_dialect)

    def as_dict(self):
        "The minimal representation of the blocking rule"
        output = {}

        output["blocking_rule"] = self.blocking_rule_sql
        output["sql_dialect"] = self.sql_dialect

        if self.salting_partitions > 1 and self.sql_dialect == "spark":
            output["salting_partitions"] = self.salting_partitions

        if self.array_columns_to_explode:
            output["arrays_to_explode"] = self.array_columns_to_explode

        return output

    def _as_completed_dict(self):
        if not self.salting_partitions > 1 and self.sql_dialect == "spark":
            return self.blocking_rule_sql
        else:
            return self.as_dict()

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


class ExplodingBlockingRule(BlockingRule):
    def __init__(
        self,
        blocking_rule: BlockingRule | dict | str,
        salting_partitions=1,
        sqlglot_dialect: str = None,
        array_columns_to_explode: list = [],
    ):
        super().__init__(blocking_rule, salting_partitions, sqlglot_dialect)
        self.array_columns_to_explode: List[str] = array_columns_to_explode
        self.exploded_id_pair_tables: List[SplinkDataFrame] = []

    def marginal_exploded_id_pairs_table_sql(
        self, linker: Linker, salted_br: BlockingRule
    ):
        """generates a table of the marginal id pairs from the exploded blocking rule
        i.e. pairs are only created that match this blocking rule and NOT any of
        the preceding blocking rules
        """
        settings_obj = linker._settings_obj
        unique_id_col = settings_obj._unique_id_column_name

        link_type = settings_obj._link_type

        if linker._two_dataset_link_only:
            link_type = "two_dataset_link_only"

        if linker._self_link_mode:
            link_type = "self_link"

        where_condition = _sql_gen_where_condition(
            link_type, settings_obj._unique_id_input_columns
        )

        if link_type == "two_dataset_link_only":
            where_condition = (
                where_condition + " and l.source_dataset < r.source_dataset"
            )

        sql = f"""
            select distinct
                l.{unique_id_col} as {unique_id_col}_l,
                r.{unique_id_col} as {unique_id_col}_r
            from __splink__df_concat_with_tf_unnested as l
            inner join __splink__df_concat_with_tf_unnested as r
            on ({salted_br.blocking_rule_sql})
            {where_condition}
            {self.exclude_pairs_generated_by_all_preceding_rules_sql(linker)}"""

        return sql

    def drop_materialised_id_pairs_dataframes(self):
        for df in self.exploded_id_pair_tables:
            df.drop_table_from_database_and_remove_from_cache()

    def exclude_pairs_generated_by_this_rule_sql(self, linker: Linker):
        """A SQL string specifying how to exclude the results
        of THIS blocking rule from subseqent blocking statements,
        so that subsequent statements do not produce duplicate pairs
        """

        unique_id_column = linker._settings_obj._unique_id_column_name

        ids_to_compare_sql = " union all ".join(
            [
                f"select * from {ids.physical_name}"
                for ids in self.exploded_id_pair_tables
            ]
        )

        return f"""EXISTS (
            select 1 from ({ids_to_compare_sql}) as ids_to_compare
            where (
                l.{unique_id_column} = ids_to_compare.{unique_id_column}_l and
                r.{unique_id_column} = ids_to_compare.{unique_id_column}_r
            )
        )
        """


class SaltedBlockingRuleSegment:
    def __init__(
        self,
        parent_blocking_rule: BlockingRule,
        blocking_rule_sql: str,
        salt: int = None,
        exploded_id_pairs_table: SplinkDataFrame = None,
    ):
        self.parent_blocking_rule = parent_blocking_rule
        self.blocking_rule_sql = blocking_rule_sql
        self.salt = salt
        self.exploded_id_pairs_tables = exploded_id_pairs_table

    @property
    def is_salted(self):
        return self.parent_blocking_rule.is_salted


def _sql_gen_where_condition(link_type, unique_id_cols):
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
            f"and l.{source_dataset_col.name()} != r.{source_dataset_col.name()}"
        )

    return where_condition


def materialise_exploded_id_tables(linker: Linker):
    settings_obj = linker._settings_obj

    blocking_rules = settings_obj._blocking_rules_to_generate_predictions
    blocking_rules = [
        br for br in blocking_rules if isinstance(br, ExplodingBlockingRule)
    ]
    salted_exploded_blocking_rules = (
        salted_br for br in blocking_rules for salted_br in br.salted_blocking_rules
    )

    for salted_br in salted_exploded_blocking_rules:
        parent_br = salted_br.parent_blocking_rule

        input_dataframe = linker._initialise_df_concat_with_tf()

        input_colnames = {col.name() for col in input_dataframe.columns}
        arrays_to_explode_quoted = [
            InputColumn(colname, sql_dialect=linker._sql_dialect).quote().name()
            for colname in parent_br.array_columns_to_explode
        ]
        expl_sql = linker._gen_explode_sql(
            "__splink__df_concat_with_tf",
            parent_br.array_columns_to_explode,
            list(input_colnames.difference(arrays_to_explode_quoted)),
        )

        linker._enqueue_sql(
            expl_sql,
            "__splink__df_concat_with_tf_unnested",
        )

        salt_name = ""
        if salted_br.is_salted:
            salt_name = f"_salt_{salted_br.salt}"

        base_name = "__splink__marginal_exploded_ids_blocking_rule"
        table_name = f"{base_name}_mk_{parent_br.match_key}{salt_name}"

        sql = parent_br.marginal_exploded_id_pairs_table_sql(linker, salted_br)

        linker._enqueue_sql(sql, table_name)

        marginal_ids_table = linker._execute_sql_pipeline([input_dataframe])
        parent_br.exploded_id_pair_tables.append(marginal_ids_table)


def block_using_rules_sqls(linker: Linker):
    """Use the blocking rules specified in the linker's settings object to
    generate a SQL statement that will create pairwise record comparions
    according to the blocking rule(s).

    Where there are multiple blocking rules, the SQL statement contains logic
    so that duplicate comparisons are not generated.
    """

    sqls = []

    # For the two dataset link only, rather than a self join of
    # __splink__df_concat_with_tf, it's much faster to split the input
    # into two tables, and join (because then Splink doesn't have to evaluate)
    # intra-dataset comparisons.
    # see https://github.com/moj-analytical-services/splink/pull/1359
    if (
        linker._two_dataset_link_only
        and not linker._find_new_matches_mode
        and not linker._compare_two_records_mode
    ):
        source_dataset_col = linker._source_dataset_column_name
        # Need df_l to be the one with the lowest id to preeserve the property
        # that the left dataset is the one with the lowest concatenated id
        keys = linker._input_tables_dict.keys()
        keys = list(sorted(keys))
        df_l = linker._input_tables_dict[keys[0]]
        df_r = linker._input_tables_dict[keys[1]]

        # This also needs to work for training u
        if linker._train_u_using_random_sample_mode:
            spl_switch = "_sample"
        else:
            spl_switch = ""

        sql = f"""
        select * from __splink__df_concat_with_tf{spl_switch}
        where {source_dataset_col} = '{df_l.templated_name}'
        """
        sqls.append(
            {
                "sql": sql,
                "output_table_name": f"__splink__df_concat_with_tf{spl_switch}_left",
            }
        )

        sql = f"""
        select * from __splink__df_concat_with_tf{spl_switch}
        where {source_dataset_col} = '{df_r.templated_name}'
        """
        sqls.append(
            {
                "sql": sql,
                "output_table_name": f"__splink__df_concat_with_tf{spl_switch}_right",
            }
        )

    if type(linker).__name__ in ["SparkLinker"]:
        apply_salt = True
    else:
        apply_salt = False

    settings_obj = linker._settings_obj

    columns_to_select = settings_obj._columns_to_select_for_blocking
    sql_select_expr = ", ".join(columns_to_select)

    link_type = settings_obj._link_type

    if linker._two_dataset_link_only:
        link_type = "two_dataset_link_only"

    if linker._self_link_mode:
        link_type = "self_link"

    where_condition = _sql_gen_where_condition(
        link_type, settings_obj._unique_id_input_columns
    )

    # We could have had a single 'blocking rule'
    # property on the settings object, and avoided this logic but I wanted to be very
    # explicit about the difference between blocking for training
    # and blocking for predictions
    if settings_obj._blocking_rule_for_training:
        blocking_rules = [settings_obj._blocking_rule_for_training]
    else:
        blocking_rules = settings_obj._blocking_rules_to_generate_predictions

    # Cover the case where there are no blocking rules
    # This is a bit of a hack where if you do a self-join on 'true'
    # you create a cartesian product, rather than having separate code
    # that generates a cross join for the case of no blocking rules
    if not blocking_rules:
        blocking_rules = [BlockingRule("1=1")]

    # For Blocking rules for deterministic rules, add a match probability
    # column with all probabilities set to 1.
    if linker._deterministic_link_mode:
        probability = ", 1.00 as match_probability"
    else:
        probability = ""

    br_sqls = []
    salted_blocking_rules = (
        salted_br for br in blocking_rules for salted_br in br.salted_blocking_rules
    )
    for salted_br in salted_blocking_rules:
        parent_br = salted_br.parent_blocking_rule
        if not isinstance(parent_br, ExplodingBlockingRule):
            sql = f"""
            select
            {sql_select_expr}
            , '{parent_br.match_key}' as match_key
            {probability}
            from {linker._input_tablename_l} as l
            inner join {linker._input_tablename_r} as r
            on
            ({salted_br.blocking_rule_sql})
            {where_condition}
            {parent_br.exclude_pairs_generated_by_all_preceding_rules_sql(linker)}
            """
        else:
            ids_to_compare = salted_br.exploded_id_pair_table
            unique_id_col = settings_obj._unique_id_column_name
            sql = f"""
                select
                    {sql_select_expr},
                    '{parent_br.match_key}' as match_key
                    {probability}
                from {ids_to_compare.physical_name} as pairs
                left join {linker._input_tablename_l} as l
                    on pairs.{unique_id_col}_l=l.{unique_id_col}
                left join {linker._input_tablename_r} as r
                    on pairs.{unique_id_col}_r=r.{unique_id_col}
            """
        br_sqls.append(sql)

    sql = "union all".join(br_sqls)

    sqls.append({"sql": sql, "output_table_name": "__splink__df_blocked"})
    return sqls

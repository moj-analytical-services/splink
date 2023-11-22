from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

from sqlglot import parse_one
from sqlglot.expressions import Column, Join
from sqlglot.optimizer.eliminate_joins import join_condition

from .misc import ensure_is_list
from .unique_id_concat import _composite_unique_id_from_nodes_sql

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

        salting_partitions = br.get("salting_partitions", None)
        if salting_partitions is None:
            return BlockingRule(blocking_rule, sqlglot_dialect)
        else:
            return SaltedBlockingRule(
                blocking_rule, sqlglot_dialect, salting_partitions
            )

    else:
        br = BlockingRule(br)
        return br


class BlockingRule:
    def __init__(
        self,
        blocking_rule_sql: str,
        sqlglot_dialect: str = None,
    ):
        if sqlglot_dialect:
            self._sql_dialect = sqlglot_dialect

        # Temporarily just to see if tests still pass
        if not isinstance(blocking_rule_sql, str):
            raise ValueError(
                f"Blocking rule must be a string, not {type(blocking_rule_sql)}"
            )
        self.blocking_rule_sql = blocking_rule_sql
        self.preceding_rules: List[BlockingRule] = []
        self.sqlglot_dialect = sqlglot_dialect

    @property
    def sql_dialect(self):
        return None if not hasattr(self, "_sql_dialect") else self._sql_dialect

    @property
    def match_key(self):
        return len(self.preceding_rules)

    def add_preceding_rules(self, rules):
        rules = ensure_is_list(rules)
        self.preceding_rules = rules

    @property
    def exclude_pairs_generated_by_this_rule_sql(self):
        """A SQL string specifying how to exclude the results
        of THIS blocking rule from subseqent blocking statements,
        so that subsequent statements do not produce duplicate pairs
        """

        # Note the coalesce function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        return f"coalesce(({self.blocking_rule_sql}),false)"

    @property
    def exclude_pairs_generated_by_all_preceding_rules_sql(self):
        """A SQL string that excludes the results of ALL previous blocking rules from
        the pairwise comparisons generated.
        """
        if not self.preceding_rules:
            return ""
        or_clauses = [
            br.exclude_pairs_generated_by_this_rule_sql for br in self.preceding_rules
        ]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"

    def create_blocked_pairs_sql(self, linker: Linker, where_condition, probability):
        columns_to_select = linker._settings_obj._columns_to_select_for_blocking
        sql_select_expr = ", ".join(columns_to_select)

        sql = f"""
            select
            {sql_select_expr}
            , '{self.match_key}' as match_key
            {probability}
            from {linker._input_tablename_l} as l
            inner join {linker._input_tablename_r} as r
            on
            ({self.blocking_rule_sql})
            {self.exclude_pairs_generated_by_all_preceding_rules_sql}
            {where_condition}
            """
        return sql

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
        sqlglot_dialect: str = None,
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

    def create_blocked_pairs_sql(self, linker: Linker, where_condition, probability):
        columns_to_select = linker._settings_obj._columns_to_select_for_blocking
        sql_select_expr = ", ".join(columns_to_select)

        sqls = []
        for salt in range(self.salting_partitions):
            salt_condition = self._salting_condition(salt)
            sql = f"""
            select
            {sql_select_expr}
            , '{self.match_key}' as match_key
            {probability}
            from {linker._input_tablename_l} as l
            inner join {linker._input_tablename_r} as r
            on
            ({self.blocking_rule_sql} {salt_condition})
            {self.exclude_pairs_generated_by_all_preceding_rules_sql}
            {where_condition}
            """

            sqls.append(sql)
        return " UNION ALL ".join(sqls)


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
            f"and l.{source_dataset_col.name} != r.{source_dataset_col.name}"
        )

    return where_condition


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
        source_dataset_col = (
            source_dataset_col
        ) = linker._settings_obj._source_dataset_column_name
        # Need df_l to be the one with the lowest id to preeserve the property
        # that the left dataset is the one with the lowest concatenated id

        # This also needs to work for training u
        if linker._train_u_using_random_sample_mode:
            spl_switch = "_sample"
        else:
            spl_switch = ""

        df_concat_tf = linker._intermediate_table_cache["__splink__df_concat_with_tf"]

        sql = f"""
        select * from __splink__df_concat_with_tf{spl_switch}
        where {source_dataset_col} =
            (select min({source_dataset_col}) from {df_concat_tf.physical_name})
        """
        sqls.append(
            {
                "sql": sql,
                "output_table_name": f"__splink__df_concat_with_tf{spl_switch}_left",
            }
        )

        sql = f"""
        select * from __splink__df_concat_with_tf{spl_switch}
        where {source_dataset_col} =
            (select max({source_dataset_col}) from {df_concat_tf.physical_name})
        """
        sqls.append(
            {
                "sql": sql,
                "output_table_name": f"__splink__df_concat_with_tf{spl_switch}_right",
            }
        )

    settings_obj = linker._settings_obj

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

    for br in blocking_rules:
        sql = br.create_blocked_pairs_sql(linker, where_condition, probability)
        br_sqls.append(sql)

    sql = " UNION ALL ".join(br_sqls)

    sqls.append({"sql": sql, "output_table_name": "__splink__df_blocked"})

    return sqls

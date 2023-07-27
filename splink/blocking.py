from __future__ import annotations

from sqlglot import parse_one
from sqlglot.expressions import Join, Column
from sqlglot.optimizer.eliminate_joins import join_condition
from typing import TYPE_CHECKING, Union
import logging

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
        salting_partitions = br.get("salting_partitions", 1)

        return BlockingRule(blocking_rule, salting_partitions)

    else:
        br = BlockingRule(br)
        return br


class BlockingRule:
    def __init__(
        self,
        blocking_rule: BlockingRule | dict | str,
        salting_partitions=1,
        sqlglot_dialect: str = None,
    ):
        if sqlglot_dialect:
            self._sql_dialect = sqlglot_dialect

        self.blocking_rule = blocking_rule
        self.preceding_rules = []
        self.sqlglot_dialect = sqlglot_dialect
        self.salting_partitions = salting_partitions

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
    def and_not_preceding_rules_sql(self):
        if not self.preceding_rules:
            return ""

        # Note the coalesce function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        or_clauses = [
            f"coalesce(({r.blocking_rule}), false)" for r in self.preceding_rules
        ]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"

    @property
    def salted_blocking_rules(self):
        if self.salting_partitions == 1:
            yield self.blocking_rule
        else:
            for n in range(self.salting_partitions):
                yield f"{self.blocking_rule} and ceiling(l.__splink_salt * {self.salting_partitions}) = {n+1}"  # noqa: E501

    @property
    def _parsed_join_condition(self):
        br = self.blocking_rule
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

        output["blocking_rule"] = self.blocking_rule

        if self.salting_partitions > 1 and self.sql_dialect == "spark":
            output["salting_partitions"] = self.salting_partitions

        return output

    @property
    def descr(self):
        return "Custom" if not hasattr(self, "_description") else self._description

    def _abbreviated_sql(self, cutoff=75):
        sql = self.blocking_rule
        return (sql[:cutoff] + "...") if len(sql) > cutoff else sql

    def __repr__(self):
        return f"<{self._human_readable_succinct}>"

    @property
    def _human_readable_succinct(self):
        sql = self._abbreviated_sql(75)
        return f"{self.descr} blocking rule using SQL: {sql}"


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


# flake8: noqa: C901
def block_using_rules_sql(linker: Linker):
    """Use the blocking rules specified in the linker's settings object to
    generate a SQL statement that will create pairwise record comparions
    according to the blocking rule(s).

    Where there are multiple blocking rules, the SQL statement contains logic
    so that duplicate comparisons are not generated.
    """

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

    if settings_obj.salting_required and apply_salt == False:
        logger.warning(
            "WARNING: Salting is not currently supported by this linker backend and"
            " will not be implemented for this run."
        )

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

        if linker._train_u_using_random_sample_mode:
            sample_switch = "_sample"
        else:
            sample_switch = ""

        sql = f"""
        select * from __splink__df_concat_with_tf{sample_switch}
        where {source_dataset_col} = '{df_l.templated_name}'
        """
        linker._enqueue_sql(sql, f"__splink__df_concat_with_tf{sample_switch}_left")

        sql = f"""
        select * from __splink__df_concat_with_tf{sample_switch}
        where {source_dataset_col} = '{df_r.templated_name}'
        """
        linker._enqueue_sql(sql, f"__splink__df_concat_with_tf{sample_switch}_right")

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

    sqls = []
    for br in blocking_rules:
        # Apply our salted rules to resolve skew issues. If no salt was
        # selected to be added, then apply the initial blocking rule.
        if apply_salt:
            salted_blocking_rules = br.salted_blocking_rules
        else:
            salted_blocking_rules = [br.blocking_rule]

        for salted_br in salted_blocking_rules:
            sql = f"""
            select
            {sql_select_expr}
            , '{br.match_key}' as match_key
            {probability}
            from {linker._input_tablename_l} as l
            inner join {linker._input_tablename_r} as r
            on
            ({salted_br})
            {br.and_not_preceding_rules_sql}
            {where_condition}
            """

            sqls.append(sql)

    sql = "union all".join(sqls)

    return sql

from typing import TYPE_CHECKING, List
import logging

from .unique_id_concat import _composite_unique_id_from_nodes_sql

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


class BlockingRule:
    def __init__(
        self,
        blocking_rule,
        salting_partitions=1,
    ):
        self.blocking_rule = blocking_rule
        self.preceding_rules = []
        self.salting_partitions = salting_partitions

    @property
    def match_key(self):
        return len(self.preceding_rules)

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
def block_using_rules_sql(linker: "Linker"):
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

    # Cover the case where there are no blocking rules
    # This is a bit of a hack where if you do a self-join on 'true'
    # you create a cartesian product, rather than having separate code
    # that generates a cross join for the case of no blocking rules
    if not blocking_rules:
        blocking_rules = [BlockingRule("1=1")]

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
            from {linker._input_tablename_l} as l
            inner join {linker._input_tablename_r} as r
            on
            {salted_br}
            {br.and_not_preceding_rules_sql}
            {where_condition}
            """

            sqls.append(sql)

    sql = "union all".join(sqls)

    return sql

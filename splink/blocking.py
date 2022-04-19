import logging

logger = logging.getLogger(__name__)


def _sql_gen_and_not_previous_rules(previous_rules: list):
    if previous_rules:
        # Note the isnull function is important here - otherwise
        # you filter out any records with nulls in the previous rules
        # meaning these comparisons get lost
        or_clauses = [f"coalesce(({r}), false)" for r in previous_rules]
        previous_rules = " OR ".join(or_clauses)
        return f"AND NOT ({previous_rules})"
    else:
        return ""


def _sql_gen_composite_unique_id(unique_id_cols, l_or_r):
    if len(unique_id_cols) == 1:
        return f"{l_or_r}.{unique_id_cols[0].name}"
    cols = [f"{l_or_r}.{c.name}" for c in unique_id_cols]

    return " || '-__-' || ".join(cols)


def _sql_gen_where_condition(link_type, unique_id_cols):

    id_expr_l = _sql_gen_composite_unique_id(unique_id_cols, "l")
    id_expr_r = _sql_gen_composite_unique_id(unique_id_cols, "r")

    if link_type == "two_dataset_link_only":
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


def block_using_rules_sql(linker):

    settings_obj = linker.settings_obj

    columns_to_select = settings_obj._columns_to_select_for_blocking
    sql_select_expr = ", ".join(columns_to_select)

    link_type = settings_obj._link_type

    if linker.two_dataset_link_only:
        link_type = "two_dataset_link_only"
    where_condition = _sql_gen_where_condition(
        link_type, settings_obj._unique_id_input_columns
    )

    sqls = []
    previous_rules = []

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
        blocking_rules = ["1=1"]

    for matchkey_number, rule in enumerate(blocking_rules):
        not_previous_rules_statement = _sql_gen_and_not_previous_rules(previous_rules)

        sql = f"""
        select
        {sql_select_expr}
        , '{matchkey_number}' as match_key
        from {linker._input_tablename_l} as l
        inner join {linker._input_tablename_r} as r
        on
        {rule}
        {not_previous_rules_statement}
        {where_condition}
        """
        previous_rules.append(rule)
        sqls.append(sql)

    sql = "union all".join(sqls)

    if not settings_obj._needs_matchkey_column:
        sql = sql.replace(", '0' as match_key", "")

    return sql

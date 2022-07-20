from typing import TYPE_CHECKING

from .blocking import _sql_gen_where_condition
from .settings import Settings
from .comparison_library import exact_match
from .misc import calculate_cartesian, calculate_reduction_ratio
from .blocking import BlockingRule

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def generate_blocking_rule_where_condition(
    linker: "Linker", link_type=None, unique_id_column_name=None
) -> str:

    if linker._settings_obj_ is not None:
        settings_obj = linker._settings_obj

    if link_type is None and linker._settings_obj_ is None:
        if len(linker._input_tables_dict.values()) == 1:
            link_type = "dedupe_only"

    if link_type is not None:
        # Minimal settings dict
        if unique_id_column_name is None:
            raise ValueError(
                "If settings not provided, you must specify unique_id_column_name"
            )
        settings_obj = Settings(
            {
                "unique_id_column_name": unique_id_column_name,
                "link_type": link_type,
                "comparisons": [exact_match("first_name")],
            }
        )

    # If link type not specified or inferrable, raise error
    if link_type is None:
        if linker._settings_obj_ is None:
            raise ValueError(
                "Must provide a link_type argument to analyse_blocking_rule_sql "
                "if linker has no settings object"
            )

    where_condition = _sql_gen_where_condition(
        settings_obj._link_type, settings_obj._unique_id_input_columns
    )

    return where_condition


def number_of_comparisons_generated_by_blocking_rule_sql(
    linker: "Linker", blocking_rule, link_type=None, unique_id_column_name=None
) -> str:

    where_condition = generate_blocking_rule_where_condition(
        linker,
        link_type,
        unique_id_column_name,
    )

    sql = f"""
    select count(*) as count_of_pairwise_comparisons_generated

    from __splink__df_concat as l
    inner join __splink__df_concat as r
    on
    {blocking_rule}
    {where_condition}
    """

    return sql


def cumulative_comparisons_generated_by_blocking_rules(
    linker: "Linker",
    concat_df,
    blocking_rules,
    link_type=None,
    unique_id_column_name=None,
):

    where_condition = generate_blocking_rule_where_condition(
        linker,
        link_type,
        unique_id_column_name,
    )

    if isinstance(blocking_rules[0], BlockingRule):
        brs_as_objs = blocking_rules
    else:
        brs_as_objs = []
        for br in blocking_rules:
            br = BlockingRule(br)
            br.preceding_rules = brs_as_objs.copy()
            brs_as_objs.append(br)

    # Calculate the Cartesian Product
    sql = f"""
        select count(*) as count
        from {concat_df}
    """
    df_count = linker._enqueue_and_execute_sql_pipeline(
        sql, "__splink__analyse_blocking_rule"
    )
    row_count_df = df_count.as_record_dict()[0]
    df_count.drop_table_from_database()

    cartesian = calculate_cartesian(row_count_df["count"])

    br_comparisons = []
    cumulative_sum = 0

    # return sqls
    for br in brs_as_objs:
        sql = f"""
        select count(*) as row_count
        from {concat_df} as l
        inner join {concat_df} as r
        on
        {br.blocking_rule}
        {where_condition}
        {br.and_not_preceding_rules_sql}
        """

        records = linker._enqueue_and_execute_sql_pipeline(
            sql, "__splink__analyse_blocking_rule"
        )
        row_count = records.as_record_dict()[0]
        records.drop_table_from_database()

        start = cumulative_sum  # starting location for gantt row
        cumulative_sum += row_count["row_count"]

        rr = round(calculate_reduction_ratio(cumulative_sum, cartesian), 3)

        rr_text = f"""
            The rolling reduction ratio with your given blocking rule(s) is {rr}.
            This represents the reduction in the total number of comparisons due
            to your rule(s).
        """

        out_dict = {
            **row_count,
            **{
                "rule": br.blocking_rule,
                "cumulative_rows": cumulative_sum,
                "cartesian": int(cartesian),
                "reduction_ratio": rr_text,
                "start": start,
            },
        }
        br_comparisons.append(out_dict.copy())

    return br_comparisons

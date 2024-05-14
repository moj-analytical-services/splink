from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable, List, Literal, Union

import pandas as pd
import sqlglot

from .blocking import (
    BlockingRule,
    _sql_gen_where_condition,
    block_using_rules_sqls,
    materialise_exploded_id_tables,
)
from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_creator_utils import to_blocking_rule_creator
from .charts import cumulative_blocking_rule_comparisons_generated
from .database_api import DatabaseAPI, DatabaseAPISubClass
from .input_column import InputColumn
from .misc import calculate_cartesian
from .pipeline import CTEPipeline
from .splink_dataframe import SplinkDataFrame
from .vertically_concatenate import (
    enqueue_df_concat,
    split_df_concat_with_tf_into_two_tables_sqls,
    vertically_concatenate_sql,
)

if TYPE_CHECKING:
    from .linker import Linker

logger = logging.getLogger(__name__)
blocking_rule_or_rules_type = Union[
    BlockingRuleCreator, str, dict, List[Union[BlockingRuleCreator, str, dict]]
]

link_type_type = Literal["link_only", "link_and_dedupe", "dedupe_only"]


def number_of_comparisons_generated_by_blocking_rule_post_filters_sqls(
    input_data_dict: dict[str, "SplinkDataFrame"],
    blocking_rule: Union[str, "BlockingRule"],
    link_type: str,
    db_api: DatabaseAPISubClass,
    unique_id_column_name: str,
) -> str:
    input_dataframes = list(input_data_dict.values())

    if len(input_dataframes) > 1:
        unique_id_cols = [
            InputColumn(unique_id_column_name, sql_dialect=db_api.sql_dialect.name),
            InputColumn("source_dataset", sql_dialect=db_api.sql_dialect.name),
        ]
    else:
        unique_id_cols = [
            InputColumn(unique_id_column_name, sql_dialect=db_api.sql_dialect.name),
        ]
    where_condition = _sql_gen_where_condition(link_type, unique_id_cols)

    sqls = []

    two_dataset_link_only = link_type == "link_only" and len(input_dataframes) == 2

    if two_dataset_link_only:
        input_tablename_l = input_dataframes[0].physical_name
        input_tablename_r = input_dataframes[1].physical_name
    else:
        sql = vertically_concatenate_sql(
            input_data_dict, salting_required=False, source_dataset_column_name=None
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    sql = f"""
    select count(*) as count_of_pairwise_comparisons_generated

    from {input_tablename_l} as l
    inner join {input_tablename_r} as r
    on
    {blocking_rule.blocking_rule_sql}
    {where_condition}
    """
    sqls.append({"sql": sql, "output_table_name": "__splink__comparions_post_filter"})
    return sqls


def count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
    input_data_dict: dict[str, "SplinkDataFrame"],
    blocking_rule: Union[str, "BlockingRule"],
    link_type: str,
    db_api: DatabaseAPISubClass,
):
    input_dataframes = list(input_data_dict.values())

    if isinstance(blocking_rule, str):
        blocking_rule = BlockingRule(blocking_rule, sqlglot_dialect=db_api.sql_dialect)

    join_conditions = blocking_rule._equi_join_conditions
    two_dataset_link_only = link_type == "link_only" and len(input_dataframes) == 2

    sqls = []

    if two_dataset_link_only:
        input_tablename_l = input_dataframes[0].physical_name
        input_tablename_r = input_dataframes[1].physical_name
    else:
        sql = vertically_concatenate_sql(
            input_data_dict, salting_required=False, source_dataset_column_name=None
        )
        sqls.append({"sql": sql, "output_table_name": "__splink__df_concat"})

        input_tablename_l = "__splink__df_concat"
        input_tablename_r = "__splink__df_concat"

    l_cols_sel = []
    r_cols_sel = []
    l_cols_gb = []
    r_cols_gb = []
    using = []
    for (
        i,
        (l_key, r_key),
    ) in enumerate(join_conditions):
        l_cols_sel.append(f"{l_key} as key_{i}")
        r_cols_sel.append(f"{r_key} as key_{i}")
        l_cols_gb.append(l_key)
        r_cols_gb.append(r_key)
        using.append(f"key_{i}")

    l_cols_sel_str = ", ".join(l_cols_sel)
    r_cols_sel_str = ", ".join(r_cols_sel)
    l_cols_gb_str = ", ".join(l_cols_gb)
    r_cols_gb_str = ", ".join(r_cols_gb)
    using_str = ", ".join(using)

    if not join_conditions:
        if two_dataset_link_only:
            sql = f"""
            SELECT
                (SELECT COUNT(*) FROM {input_tablename_l})
                *
                (SELECT COUNT(*) FROM {input_tablename_r})
                    AS count_of_pairwise_comparisons_generated
            """
        else:
            sql = """
            select count(*) * count(*) as count_of_pairwise_comparisons_generated
            from __splink__df_concat

            """
        sqls.append(
            {"sql": sql, "output_table_name": "__splink__total_of_block_counts"}
        )
        return sqls

    sql = f"""
    select {l_cols_sel_str}, count(*) as count_l
    from {input_tablename_l}
    group by {l_cols_gb_str}
    """

    sqls.append(
        {"sql": sql, "output_table_name": "__splink__count_comparisons_from_blocking_l"}
    )

    sql = f"""
    select {r_cols_sel_str}, count(*) as count_r
    from {input_tablename_r}
    group by {r_cols_gb_str}
    """

    sqls.append(
        {"sql": sql, "output_table_name": "__splink__count_comparisons_from_blocking_r"}
    )

    sql = f"""
    select *, count_l, count_r, count_l * count_r as block_count
    from __splink__count_comparisons_from_blocking_l
    inner join __splink__count_comparisons_from_blocking_r
    using ({using_str})
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__block_counts"})

    sql = """
    select cast(sum(block_count) as integer) as count_of_pairwise_comparisons_generated
    from __splink__block_counts
    """

    sqls.append({"sql": sql, "output_table_name": "__splink__total_of_block_counts"})

    return sqls


def _row_counts_per_df(
    splink_df_dict: dict[str, "SplinkDataFrame"],
    link_type: link_type_type,
    source_dataset_column_name: str,
    unique_id_column_name: str,
    db_api: DatabaseAPI,
):
    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_column_name=source_dataset_column_name,
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    if link_type == "dedupe_only":
        sql = """
        select count(*) as count
        from __splink__df_concat
        """
    else:
        sql = f"""
        select count(*) as count
        from __splink__df_concat
        group by {source_dataset_column_name}
        """
    pipeline.enqueue_sql(sql, "__splink__df_count")
    return db_api.sql_pipeline_to_splink_dataframe(pipeline)


def count_comparisons_to_be_scored_from_blocking_rules(
    *,
    table_or_tables,
    blocking_rules: Iterable[Union[BlockingRuleCreator, str, dict]],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str,
    source_dataset_column_name: str = None,
):
    unique_id_input_column = InputColumn(
        unique_id_column_name, sql_dialect=db_api.sql_dialect.name
    )
    if link_type == "dedupe_only":
        source_dataset_input_column = None
        input_columns = [unique_id_input_column]
    else:
        source_dataset_input_column = InputColumn(
            source_dataset_column_name, sql_dialect=db_api.sql_dialect.name
        )
        input_columns = [unique_id_input_column, source_dataset_input_column]

    blocking_rules_as_brs = [
        to_blocking_rule_creator(br).get_blocking_rule(db_api.sql_dialect.name)
        for br in blocking_rules
    ]

    for n, br in enumerate(blocking_rules_as_brs):
        br.add_preceding_rules(blocking_rules_as_brs[:n])

    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    exploding_br_with_id_tables = materialise_exploded_id_tables(
        link_type,
        blocking_rules_as_brs,
        db_api,
        splink_df_dict,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
    )

    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_column_name="source_dataset",
    )
    pipeline.enqueue_sql(sql, "__splink__df_concat")

    sql_select_expr = ",".join(
        [item for c in input_columns for item in c.l_r_names_as_l_r]
    )

    blocking_input_tablename_l = "__splink__df_concat"
    blocking_input_tablename_r = "__splink__df_concat"
    if len(splink_df_dict) == 2 and link_type == "link_only":
        sqls = split_df_concat_with_tf_into_two_tables_sqls(
            "__splink__df_concat",
            source_dataset_column_name,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocking_input_tablename_l = "__splink__df_concat_left"
        blocking_input_tablename_r = "__splink__df_concat_right"
        link_type = "two_dataset_link_only"

    sqls = block_using_rules_sqls(
        input_tablename_l=blocking_input_tablename_l,
        input_tablename_r=blocking_input_tablename_r,
        blocking_rules=blocking_rules_as_brs,
        link_type="dedupe_only",
        set_match_probability_to_one=True,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
        columns_to_select_sql=sql_select_expr,
    )

    pipeline.enqueue_list_of_sqls(sqls)

    df = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_pandas_dataframe()

    [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]

    return df


def cumulative_comparisons_to_be_scored_from_blocking_rules_data(
    *,
    table_or_tables,
    blocking_rule_creators: Iterable[Union[BlockingRuleCreator, str, dict]],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str,
    source_dataset_column_name: str = None,
):
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    blocking_rules: List[BlockingRule] = []
    for br in blocking_rule_creators:
        if isinstance(br, BlockingRule):
            blocking_rules.append(br)
        else:
            blocking_rules.append(
                to_blocking_rule_creator(br).get_blocking_rule(db_api.sql_dialect.name)
            )

    return _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules,
        link_type=link_type,
        db_api=db_api,
        post_filter_limit=post_filter_limit,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
    )


def cumulative_comparisons_to_be_scored_from_blocking_rules_chart(
    *,
    table_or_tables,
    blocking_rule_creators: Iterable[Union[BlockingRuleCreator, str, dict]],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str,
    source_dataset_column_name: str = None,
):
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

    blocking_rules: List[BlockingRule] = []
    for br in blocking_rule_creators:
        if isinstance(br, BlockingRule):
            blocking_rules.append(br)
        else:
            blocking_rules.append(
                to_blocking_rule_creator(br).get_blocking_rule(db_api.sql_dialect.name)
            )

    pd_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
        splink_df_dict=splink_df_dict,
        blocking_rules=blocking_rules,
        link_type=link_type,
        db_api=db_api,
        post_filter_limit=post_filter_limit,
        unique_id_column_name=unique_id_column_name,
        source_dataset_column_name=source_dataset_column_name,
    )

    return cumulative_blocking_rule_comparisons_generated(
        pd_df.to_dict(orient="records")
    )


def _cumulative_comparisons_to_be_scored_from_blocking_rules(
    *,
    splink_df_dict: dict[str, "SplinkDataFrame"],
    blocking_rules: Iterable[BlockingRule],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str,
    source_dataset_column_name: str = None,
) -> pd.DataFrame:
    unique_id_input_column = InputColumn(
        unique_id_column_name, sql_dialect=db_api.sql_dialect.name
    )
    if link_type == "dedupe_only":
        source_dataset_input_column = None
        input_columns = [unique_id_input_column]
    else:
        source_dataset_input_column = InputColumn(
            source_dataset_column_name, sql_dialect=db_api.sql_dialect.name
        )
        input_columns = [unique_id_input_column, source_dataset_input_column]

    # Check none of the blocking rules will create a vast/computationally
    # intractable number of comparisons
    for br in blocking_rules:
        # TODO: Deal properly with exlpoding rules
        count = _count_comparisons_generated_from_blocking_rule(
            splink_df_dict,
            blocking_rule=br,
            link_type=link_type,
            db_api=db_api,
            post_filter_limit=post_filter_limit,
            compute_post_filter_count=False,
            unique_id_column_name=unique_id_column_name,
        )
        count_pre_filter = count["number_of_comparison_pre_filter_conditions"]

        if count_pre_filter > post_filter_limit:
            # TODO: Use a SplinkException?  Want this to give a sensible message
            # when ocoming from estimate_probability_two_random_records_match
            raise ValueError(
                f"Blocking rule {br.blocking_rule_sql} would create {count_pre_filter} "
                "comparisonns.\nThis exceeds the post_filter_limit of "
                f"{post_filter_limit}.\nPlease tighten the "
                "blocking rule or increase the post_filter_limit."
            )

    rc = _row_counts_per_df(
        splink_df_dict,
        link_type,
        source_dataset_column_name,
        unique_id_column_name,
        db_api,
    ).as_record_dict()

    cartesian_count = calculate_cartesian(rc, link_type)

    for n, br in enumerate(blocking_rules):
        br.add_preceding_rules(blocking_rules[:n])

    exploding_br_with_id_tables = materialise_exploded_id_tables(
        link_type,
        blocking_rules,
        db_api,
        splink_df_dict,
        source_dataset_input_column=source_dataset_input_column,
        unique_id_input_column=unique_id_input_column,
    )

    pipeline = CTEPipeline()

    sql = vertically_concatenate_sql(
        splink_df_dict,
        salting_required=False,
        source_dataset_column_name=source_dataset_column_name,
    )

    pipeline.enqueue_sql(sql, "__splink__df_concat")

    sql_select_expr = ",".join(
        [item for c in input_columns for item in c.l_r_names_as_l_r]
    )

    blocking_input_tablename_l = "__splink__df_concat"
    blocking_input_tablename_r = "__splink__df_concat"
    if len(splink_df_dict) == 2 and link_type == "link_only":
        sqls = split_df_concat_with_tf_into_two_tables_sqls(
            "__splink__df_concat",
            source_dataset_column_name,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocking_input_tablename_l = "__splink__df_concat_left"
        blocking_input_tablename_r = "__splink__df_concat_right"
        link_type = "two_dataset_link_only"

    sqls = block_using_rules_sqls(
        input_tablename_l=blocking_input_tablename_l,
        input_tablename_r=blocking_input_tablename_r,
        blocking_rules=blocking_rules,
        link_type="dedupe_only",
        set_match_probability_to_one=True,
        unique_id_input_column=unique_id_input_column,
        source_dataset_input_column=source_dataset_input_column,
        columns_to_select_sql=sql_select_expr,
    )

    pipeline.enqueue_list_of_sqls(sqls)

    sql = """
        select
        count(*) as row_count,
        match_key
        from __splink__df_blocked
        group by match_key
        order by cast(match_key as int) asc
    """
    pipeline.enqueue_sql(sql, "__splink__df_count_cumulative_blocks")

    sql = f"""
    SELECT
        row_count,
        match_key,
        cast(SUM(row_count) OVER (ORDER BY match_key) as int) AS cumulative_rows,
        cast(SUM(row_count) OVER (ORDER BY match_key) - row_count as int) AS start,
        cast({cartesian_count} as int) as cartesian

    FROM
        __splink__df_count_cumulative_blocks
    """

    pipeline.enqueue_sql(sql, "__splink__df_count_cumulative_blocks_2")

    records = db_api.sql_pipeline_to_splink_dataframe(pipeline).as_record_dict()

    # Lookup table match_key -> blocking_rule
    rules = {i: r.blocking_rule_sql for i, r in enumerate(blocking_rules)}

    for r in records:
        r["blocking_rule"] = rules[int(r["match_key"])]

    [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]

    col_order = [
        "blocking_rule",
        "row_count",
        "cumulative_rows",
        "cartesian",
        "match_key",
        "start",
    ]
    return pd.DataFrame(records)[col_order]


def count_comparisons_generated_from_blocking_rule(
    table_or_tables,
    *,
    blocking_rule: Union[BlockingRule, BlockingRuleCreator, str, dict],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    compute_post_filter_count: bool = False,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str = "unique_id",
):
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)
    return _count_comparisons_generated_from_blocking_rule(
        splink_df_dict=splink_df_dict,
        blocking_rule=blocking_rule,
        link_type=link_type,
        db_api=db_api,
        compute_post_filter_count=compute_post_filter_count,
        post_filter_limit=post_filter_limit,
        unique_id_column_name=unique_id_column_name,
    )


def _count_comparisons_generated_from_blocking_rule(
    splink_df_dict: dict[str, "SplinkDataFrame"],
    *,
    blocking_rule: Union[BlockingRule, BlockingRuleCreator, str, dict],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    compute_post_filter_count: bool = False,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str = "unique_id",
):
    # TODO: if it's an exploding blocking rule, make sure we error out

    if not isinstance(blocking_rule, BlockingRule):
        blocking_rule = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
            db_api.sql_dialect.name
        )
    pipeline = CTEPipeline()
    sqls = count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
        splink_df_dict, blocking_rule, link_type, db_api
    )
    pipeline.enqueue_list_of_sqls(sqls)
    pre_filter_total_df = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    pre_filter_total = pre_filter_total_df.as_record_dict()[0][
        "count_of_pairwise_comparisons_generated"
    ]
    pre_filter_total_df.drop_table_from_database_and_remove_from_cache()

    def add_l_r(sql, table_name):
        tree = sqlglot.parse_one(sql, dialect=db_api.sql_dialect.sqlglot_name)
        for node in tree.find_all(sqlglot.expressions.Column):
            node.set("table", table_name)
        return tree.sql(dialect=db_api.sql_dialect.sqlglot_name)

    equi_join_conditions = [
        add_l_r(i, "l") + " = " + add_l_r(j, "r")
        for i, j in blocking_rule._equi_join_conditions
    ]

    equi_join_conditions = " AND ".join(equi_join_conditions)

    if not compute_post_filter_count:
        return {
            "number_of_comparison_pre_filter_conditions": pre_filter_total,
            "number_of_comparison_post_filter_conditions": "not computed",
            "filter_conditions_identified": blocking_rule._filter_conditions,
            "equi_join_conditions_identified": equi_join_conditions,
        }

    if pre_filter_total < post_filter_limit:
        pipeline = CTEPipeline()
        sqls = number_of_comparisons_generated_by_blocking_rule_post_filters_sqls(
            splink_df_dict, blocking_rule, link_type, db_api, unique_id_column_name
        )
        pipeline.enqueue_list_of_sqls(sqls)
        post_filter_total_df = db_api.sql_pipeline_to_splink_dataframe(pipeline)
        post_filter_total = post_filter_total_df.as_record_dict()[0][
            "count_of_pairwise_comparisons_generated"
        ]
        post_filter_total_df.drop_table_from_database_and_remove_from_cache()
    else:
        post_filter_total = "exceeded post_filter_limit, see warning"

        logger.warning(
            "WARNING:\nComputation of number of comparisons pre-filter conditions was "
            f"skipped because it exceeded post_filter_limit={post_filter_limit:.2e}."
            "\nIt would be likely to be slow to compute.\nIf you still want to go ahead"
            " increase the value of post_filter_limit argument to above "
            f"{pre_filter_total:.3e}.\nRead more about the definitions here:\n"
            "https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html?h=filter+cond#filter-conditions"
        )

    return {
        "number_of_comparison_pre_filter_conditions": pre_filter_total,
        "number_of_comparison_post_filter_conditions": post_filter_total,
        "filter_conditions_identified": blocking_rule._filter_conditions,
        "equi_join_conditions_identified": equi_join_conditions,
    }


def count_comparisons_from_blocking_rule_pre_filter_conditions(
    linker: "Linker", blocking_rule: Union[str, "BlockingRule"]
):
    pipeline = CTEPipeline()
    pipeline = enqueue_df_concat(linker, pipeline)

    sqls = count_comparisons_from_blocking_rule_pre_filter_conditions_sqls(
        linker, blocking_rule
    )
    pipeline.enqueue_list_of_sqls(sqls)

    df_res = linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)
    res = df_res.as_record_dict()[0]
    return int(res["count_of_pairwise_comparisons_generated"])

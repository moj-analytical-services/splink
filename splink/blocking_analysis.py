import logging
from typing import DefaultDict, Dict, Iterable, List, Literal, Union

import sqlglot

from .analyse_blocking import (
    count_comparisons_from_blocking_rule_pre_filter_conditions_sqls,
    number_of_comparisons_generated_by_blocking_rule_post_filters_sqls,
)
from .blocking import block_using_rules_sqls, materialise_exploded_id_tables
from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_creator_utils import to_blocking_rule_creator
from .database_api import DatabaseAPI
from .linker import Linker
from .misc import calculate_cartesian
from .pipeline import CTEPipeline
from .settings_creator import SettingsCreator
from .splink_dataframe import SplinkDataFrame
from .vertically_concatenate import enqueue_df_concat, vertically_concatenate_sql

logger = logging.getLogger(__name__)
blocking_rule_or_rules_type = Union[
    BlockingRuleCreator, str, dict, List[Union[BlockingRuleCreator, str, dict]]
]

link_type_type = Literal["link_only", "link_and_dedupe", "dedupe_only"]


def _row_counts_per_df(linker: Linker):
    pipeline = CTEPipeline()

    pipeline = enqueue_df_concat(linker, pipeline)

    link_type = linker._settings_obj._link_type
    if link_type == "dedupe_only":
        sql = """
        select count(*) as count
        from __splink__df_concat
        """
    else:
        column_info_settings = linker._settings_obj.column_info_settings
        source_dataset_column_name = column_info_settings.source_dataset_column_name
        sql = f"""
        select count(*) as count
        from __splink__df_concat
        group by {source_dataset_column_name}
        """
    pipeline.enqueue_sql(sql, "__splink__df_count")
    return linker.db_api.sql_pipeline_to_splink_dataframe(pipeline)


def cumulative_comparisons_to_be_scored_from_blocking_rules(
    table_or_tables,
    *,
    blocking_rules: Iterable[Union[BlockingRuleCreator, str, dict]],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str = "unique_id",
    source_dataset_column_name: str = "source_dataset",
):
    # To find marginal rows created by each blocking rule, we need a linker because
    # the blocking logic in block_using_rules_sqls requires a linker
    settings = SettingsCreator(
        link_type=link_type,
        unique_id_column_name=unique_id_column_name,
        retain_intermediate_calculation_columns=False,
        retain_matching_columns=False,
        blocking_rules_to_generate_predictions=blocking_rules,
    )
    linker = Linker(
        table_or_tables,
        settings=settings,
        database_api=db_api,
    )

    rc = _row_counts_per_df(linker).as_record_dict()
    cartesian_count = calculate_cartesian(rc, link_type)

    pipeline = CTEPipeline()

    pipeline = enqueue_df_concat(linker, pipeline)

    exploding_br_with_id_tables = materialise_exploded_id_tables(linker, link_type)

    sqls = block_using_rules_sqls(
        linker,
        input_tablename_l="__splink__df_concat",
        input_tablename_r="__splink__df_concat",
        blocking_rules=linker._settings_obj._blocking_rules_to_generate_predictions,
        link_type=link_type,
        set_match_probability_to_one=True,
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
    rules = {
        i: r.blocking_rule_sql
        for i, r in enumerate(
            linker._settings_obj._blocking_rules_to_generate_predictions
        )
    }

    for r in records:
        r["blocking_rule"] = rules[int(r["match_key"])]

    [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]
    return records


def count_comparisons_generated_from_blocking_rule(
    table_or_tables,
    *,
    blocking_rule: Union[BlockingRuleCreator, str, dict],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    compute_post_filter_count: bool = False,
    post_filter_limit: int = 1e9,
    unique_id_column_name: str = "unique_id",
):
    # TODO: if it's an exploding blocking rule, make sure we error out
    splink_df_dict = db_api.register_multiple_tables(table_or_tables)

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

    return {
        "number_of_comparison_pre_filter_conditions": pre_filter_total,
        "number_of_comparison_post_filter_conditions": post_filter_total,
        "filter_conditions_identified": blocking_rule._filter_conditions,
        "equi_join_conditions_identified": equi_join_conditions,
    }

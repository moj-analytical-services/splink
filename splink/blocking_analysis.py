import logging
from typing import List, Literal, Union

from .analyse_blocking import (
    count_comparisons_from_blocking_rule_pre_filter_conditions_sqls,
    number_of_comparisons_generated_by_blocking_rule_post_filters_sqls,
)
from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_creator_utils import to_blocking_rule_creator
from .database_api import DatabaseAPI
from .pipeline import CTEPipeline

logger = logging.getLogger(__name__)
blocking_rule_or_rules_type = Union[
    BlockingRuleCreator, str, dict, List[Union[BlockingRuleCreator, str, dict]]
]

link_type_type = Literal["link_only", "link_and_dedupe", "dedupe_only"]


def cumulative_comparisons_to_be_scored_from_blocking_rules():
    pass


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
        post_filter_total = None

        logger.warning(
            "WARNING: Computation of number of comparisons pre-filter conditions was "
            f"skipped because it exceeded post_filter_limit={post_filter_limit:.2e}."
            "\nIt would be likely to be slow to compute.\nIf you still want to go ahead "
            "increase the value of post_filter_limit argument to above "
            f"{pre_filter_total:.3e}.\nRead more about the definitions here:"
            "https://moj-analytical-services.github.io/splink/topic_guides/blocking/performance.html?h=filter+cond#filter-conditions"
        )
    return {
        "number_of_comparison_pre_filter_conditions": pre_filter_total,
        "number_of_comparison_post_filter_conditions": post_filter_total,
        "filter_conditions_identified": blocking_rule._filter_conditions,
        "equi_join_conditions_identified": blocking_rule._equi_join_conditions,
    }
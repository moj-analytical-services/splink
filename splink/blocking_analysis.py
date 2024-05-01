from typing import List, Literal, Union

from .analyse_blocking import count_comparisons_from_blocking_rule_pre_filter_conditions
from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_creator_utils import to_blocking_rule_creator
from .database_api import DatabaseAPI
from .misc import ensure_is_iterable

blocking_rule_or_rules_type = Union[
    BlockingRuleCreator, str, dict, List[Union[BlockingRuleCreator, str, dict]]
]

link_type_type = Literal["link_only", "link_and_dedupe", "dedupe_only"]


def cumulative_comparisons_from_blocking_rules():
    pass


def count_comparisons_from_blocking_rule(
    table_or_tables,
    *,
    blocking_rule: Union[BlockingRuleCreator, str, dict],
    link_type: link_type_type,
    db_api: DatabaseAPI,
    compute_post_filter_count: bool = False,
    post_filter_limit: int = 1e9,
):
    blocking_rule = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
        db_api.sql_dialect
    )

    pre_filter_total = count_comparisons_from_blocking_rule_pre_filter_conditions()

    return {
        "number_of_comparison_pre_filter_conditions": pre_filter_total,
        "number_of_comparison_post_filter_conditions": 0,
    }

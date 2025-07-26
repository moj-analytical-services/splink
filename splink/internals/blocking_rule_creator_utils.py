from __future__ import annotations

from typing import Any, Iterable, List, Union

from splink.internals.blocking import BlockingRule
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.misc import ensure_is_iterable

from .blocking_rule_library import CustomRule


def to_blocking_rule_creator(
    blocking_rule_creator: Union[dict[str, Any], str, BlockingRuleCreator],
) -> BlockingRuleCreator:
    if isinstance(blocking_rule_creator, dict):
        return CustomRule(**blocking_rule_creator)
    if isinstance(blocking_rule_creator, str):
        return CustomRule(blocking_rule_creator)
    return blocking_rule_creator


def blocking_rule_args_to_list_of_blocking_rules(
    blocking_rule_args: Union[
        str, BlockingRuleCreator, List[Union[str, BlockingRuleCreator]]
    ],
    sql_dialect: str,
) -> list[BlockingRule]:
    """In functions such as
    `linker.training.estimate_probability_two_random_records_match` the
    user may have passed in strings or BlockingRuleCreator objects.

    Additionally, they may have passed in a single string or a single
    BlockingRuleCreator object instead of a list.

    This function converts the input to a list of BlockingRule objects
    """
    br_iterable: Iterable[str | BlockingRuleCreator] = ensure_is_iterable(
        blocking_rule_args
    )
    rules_as_creators = [to_blocking_rule_creator(br) for br in br_iterable]
    rules_as_brs = [br.get_blocking_rule(sql_dialect) for br in rules_as_creators]
    return rules_as_brs

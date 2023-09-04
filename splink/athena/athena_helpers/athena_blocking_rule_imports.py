from __future__ import annotations

from ...blocking_rules_library import (
    BlockingRule,
    exact_match_rule,
)
from ...blocking_rules_library import (
    block_on as _block_on_,
)
from .athena_base import (
    AthenaBase,
)


class exact_match_rule(AthenaBase, exact_match_rule):
    pass


def block_on(
    col_names: list[str],
    salting_partitions: int = 1,
) -> BlockingRule:

    return _block_on_(
        exact_match_rule,
        col_names,
        salting_partitions,
    )


block_on.__doc__ = _block_on_.__doc__

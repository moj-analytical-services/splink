from ...blocking_rules_library import (
    BlockingRule,
    block_on_columns as _block_on_columns_,
    exact_match_rule,
)
from .postgres_base import (
    PostgresBase,
)


class exact_match_rule(PostgresBase, exact_match_rule):
    pass


def block_on_columns(
    col_names: list[str],
    salting_partitions: int = 1,
) -> BlockingRule:

    return _block_on_columns_(
        exact_match_rule,
        col_names,
        salting_partitions,
    )

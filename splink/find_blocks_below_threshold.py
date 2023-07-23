import logging
import string
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd
from sqlglot import parse_one

from .input_column import InputColumn, add_table

if TYPE_CHECKING:
    from .linker import Linker
logger = logging.getLogger(__name__)


def sanitise_column_name(column_name):
    allowed_chars = string.ascii_letters + string.digits + "_"
    sanitized_name = "".join(c for c in column_name if c in allowed_chars)
    return sanitized_name


def _generate_row(blocking_columns, comparison_count, all_columns):
    row = {}

    blocking_columns = [sanitise_column_name(c) for c in blocking_columns]
    all_columns = [sanitise_column_name(c) for c in all_columns]

    row["blocking_rules"] = ", ".join(blocking_columns)
    row["comparison_count"] = f"{comparison_count:,.0f}"
    row["complexity"] = len(blocking_columns)

    for col in all_columns:
        row[f"__fixed__{col}"] = 1 if col in blocking_columns else 0

    return row


def _generate_combinations(
    all_columns, current_combination, already_visited: Set[frozenset]
):
    combinations = []
    for col in all_columns:
        if col not in current_combination:
            next_combination = current_combination + [col]
            if frozenset(next_combination) not in already_visited:
                combinations.append(next_combination)

    return combinations


def _generate_blocking_rule(linker, cols_as_string):
    # User might have inputted e.g. substr(name, 1,2)

    trees = [parse_one(c, read=linker._sql_dialect) for c in cols_as_string]
    equi_joins = [
        (add_table(tree, "l").sql(), add_table(tree, "r").sql()) for tree in trees
    ]

    br = " AND ".join([f"{item[0]} = {item[1]}" for item in equi_joins])
    return br


def _search_combinations(
    linker: "Linker",
    all_columns: List[str],
    threshold: float,
    current_combination: List[str] = None,
    already_visited: Set[frozenset] = None,
    results: List[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    """
    Recursively search combinations of fields to find ones that result in a count less than the threshold.

    Args:
        linker: splink.Linker
        fields (List[str]): List of fields to combine.
        threshold (float): The count threshold.
        current_combination (List[str], optional): Current combination of fields. Defaults to [].
        already_visited (Set[frozenset], optional): Set of visited combinations. Defaults to set().
        results (List[Dict[str, str]], optional): List of results. Defaults to [].

    Returns:
        List[Dict[str, str]]: List of results.
    """
    if current_combination is None:
        current_combination = []
    if already_visited is None:
        already_visited = set()
    if results is None:
        results = []

    if len(current_combination) == len(all_columns):
        return results  # All fields have been included, exit recursion

    br = _generate_blocking_rule(linker, current_combination)
    comparison_count = (
        linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(br)
    )
    row = _generate_row(current_combination, comparison_count, all_columns)

    already_visited.add(frozenset(current_combination))

    if comparison_count > threshold:
        # Generate all valid combinations and continue the search
        combinations = _generate_combinations(
            all_columns, current_combination, already_visited
        )
        for next_combination in combinations:
            _search_combinations(
                linker,
                all_columns,
                threshold,
                next_combination,
                already_visited,
                results,
            )
    else:
        logger.debug(
            f"Comparison count for {current_combination}: {comparison_count:,.0f}"
        )

        results.append(row)

    return results


def find_blocking_rules_below_threshold(
    linker: "Linker", max_comparisons_per_rule, columns=None
):
    if not columns:
        columns = linker._column_names_as_input_columns

    columns_as_strings = []

    for c in columns:
        if isinstance(c, InputColumn):
            columns_as_strings.append(c.quote().name())
        else:
            columns_as_strings.append(c)

    results = _search_combinations(linker, columns_as_strings, max_comparisons_per_rule)
    return pd.DataFrame(results)

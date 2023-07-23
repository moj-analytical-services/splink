import logging
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd

if TYPE_CHECKING:
    from .linker import Linker
logger = logging.getLogger(__name__)


def _generate_row(
    blocking_rules: List[str], count: float, fields: List[str]
) -> Dict[str, str]:
    """
    Generate a row with the current blocking rule, count, complexity, and field values.

    Args:
        blocking_rules (List[str]): Current combination of fields.
        count (float): Current count.
        fields (List[str]): All available fields.

    Returns:
        Dict[str, str]: Generated row.
    """
    row = {}
    row["blocking_rules"] = ", ".join(blocking_rules)
    row["count"] = f"{count:,.0f}"
    row["complexity"] = len(blocking_rules)

    for field in fields:
        row[f"field_{field}"] = 1 if field in blocking_rules else 0

    return row


def _generate_combinations(
    fields: List[str], current_combination: List[str], already_visited: Set[frozenset]
) -> List[List[str]]:
    """
    Generate all valid combinations that haven't been visited yet.

    Args:
        fields (List[str]): List of all available fields.
        current_combination (List[str]): Current combination of fields.
        already_visited (Set[frozenset]): Set of visited combinations.

    Returns:
        List[List[str]]: List of valid combinations.
    """
    combinations = []
    for field in fields:
        if field not in current_combination:
            next_combination = current_combination + [field]
            if frozenset(next_combination) not in already_visited:
                combinations.append(next_combination)

    return combinations


def _search_combinations(
    linker: "Linker",
    fields: List[str],
    threshold: float,
    current_combination: List[str] = [],
    already_visited: Set[frozenset] = set(),
    results: List[Dict[str, str]] = [],
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
    if len(current_combination) == len(fields):
        return results  # All fields have been included, exit recursion

    br = " AND ".join([f"l.{item} = r.{item}" for item in current_combination])
    current_count = (
        linker._count_num_comparisons_from_blocking_rule_pre_filter_conditions(br)
    )
    row = _generate_row(current_combination, current_count, fields)

    already_visited.add(frozenset(current_combination))

    if current_count > threshold:
        # Generate all valid combinations and continue the search
        combinations = _generate_combinations(
            fields, current_combination, already_visited
        )
        for next_combination in combinations:
            _search_combinations(
                linker, fields, threshold, next_combination, already_visited, results
            )
    else:
        logger.debug(
            f"Comparison count for {current_combination}: {current_count:,.0f}"
        )

        results.append(row)

    return results


def find_blocking_rules_below_threshold(
    linker: "Linker", max_comparisons_per_rule, fields=None
):
    # Get columns that aren't in source_dataset or
    # unique_id or additional_columns_to_retain

    # TODO:  Refactor this into its own function
    df_obj = next(iter(linker._input_tables_dict.values()))
    columns = df_obj.columns_escaped

    remove_cols = linker._settings_obj._unique_id_input_columns
    remove_cols.extend(linker._settings_obj._additional_columns_to_retain)
    remove_id_cols = [c.quote().name() for c in remove_cols]
    columns = [col for col in columns if col not in remove_id_cols]
    columns = [col.replace('"', "").replace("'", "") for col in columns]
    results = _search_combinations(linker, columns, max_comparisons_per_rule)
    return pd.DataFrame(results)

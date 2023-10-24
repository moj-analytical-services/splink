import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def calculate_field_freedom_cost(combination_of_brs: List[Dict]) -> int:
    """
    We want a higher scores (lower cost) for combinations of blocking rules that allow
    as much variation in each field as possible

    e.g.  we don't like combinations of four rules
    that hold first_name and surname constant in 3 out of 4
    and only allows them to vary in one, even if that affords greater
    variance to other fields.

    That is, we would prefer a spread in which fields are held fixed across the blocking
    rules

    Calculates the field cost for a given combination of brs. It counts the number of
    times each field is allowed to vary (i.e., not included in the blocking rules).

    Args:
        combination_of_brs (List[Dict]): The combination_of_brs rows

    Returns:
        int: The field freedom cost.
    """

    total_cost = 0
    field_names = [c for c in combination_of_brs[0].keys() if c.startswith("__fixed__")]

    for field in field_names:
        field_can_vary_count = sum(row[field] == 0 for row in combination_of_brs)

        costs_by_count = {0: 20, 1: 10, 2: 2, 3: 1, 4: 1}
        cost = costs_by_count.get(field_can_vary_count, 0) / 10

        total_cost = total_cost + cost

    return total_cost


def calculate_cost_of_combination_of_brs(
    br_combination: List[Dict],
    max_comparison_count,
    complexity_weight,
    field_freedom_weight,
    num_brs_weight,
    num_comparison_weight,
):
    """
    Calculates a cost for a given br_combination of blocking rules.

    The cost is a weighted sum of the complexity of the rules, the count of rules,
    number of fields that are allowed to vary, and number of rows.

    The combination is a subset of rows from the output of
    find_blocking_rules_below_threshold_comparison_count.

    Each row represents a blocking rule and associated infomration.


    Args:
        br_combination (list): The combination of rows outputted by
            find_blocking_rules_below_threshold_comparison_count
        complexity_weight (int, optional): The weight for complexity. Defaults to 10.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to
            10.
        num_brs_weight (int, optional): The weight for the number of blocking rules
            found. Defaults to 1000.

    Returns:
        dict: The calculated cost and individual component costs.
    """

    # Complexity is the number of fields held constant in a given blocking rule
    complexity_cost = sum(row["complexity"] for row in br_combination)
    total_row_count = sum(row["comparison_count"] for row in br_combination)
    normalised_row_count = total_row_count / max_comparison_count

    # We want a better score for br_combinations that allow each field to
    # vary as much as possible.
    field_freedom_cost = calculate_field_freedom_cost(br_combination)
    num_brs_cost = len(br_combination)

    complexity_cost_weighted = complexity_weight * complexity_cost
    field_freedom_cost_weighted = field_freedom_weight * field_freedom_cost
    num_brs_cost_weighted = num_brs_weight * num_brs_cost
    num_comparison_rows_cost_weighted = num_comparison_weight * normalised_row_count

    total_cost = (
        complexity_cost_weighted
        + field_freedom_cost_weighted
        + num_brs_cost_weighted
        + num_comparison_rows_cost_weighted
    )

    return {
        "cost": total_cost,
        "total_pairwise_rows_created": total_row_count,
        "complexity_cost_weighted": complexity_cost_weighted,
        "field_freedom_cost_weighted": field_freedom_cost_weighted,
        "num_brs_cost_weighted": num_brs_cost_weighted,
        "num_comparison_rows_cost_weighted": num_comparison_rows_cost_weighted,
        "complexity_cost": complexity_cost,
        "field_freedom_cost": field_freedom_cost,
        "num_brs_cost": num_brs_cost,
        "num_comparison_rows_cost": normalised_row_count,
    }

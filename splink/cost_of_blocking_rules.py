import logging
from typing import Dict, List, Union

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

    # This lookup is somewhat arbitary but its purpose is to assign a very high
    # cost to combinations of blocking rules where a a field is not allowed to vary
    # much
    # TODO: Could incorporate information about how many other fields are allowed
    # to vary i.e. it's not just the count of other blocking rules that allow this
    # field to matter ,it's also how strict they are
    costs_by_count = {0: 20, 1: 10, 2: 2, 3: 1, 4: 1}

    for field in field_names:
        field_can_vary_count = sum(row[field] == 0 for row in combination_of_brs)

        cost = costs_by_count.get(field_can_vary_count, 0) / 10

        total_cost = total_cost + cost

    return total_cost


def calculate_cost_of_combination_of_brs(
    br_combination: List[Dict],
    max_comparison_count: int,
    num_equi_join_weight: Union[int, float] = 1,
    field_freedom_weight: Union[int, float] = 1,
    num_brs_weight: Union[int, float] = 1,
    num_comparison_weight: Union[int, float] = 1,
) -> dict:
    """
    Calculates the cost for a given combination of blocking rules.

    The cost is a weighted sum of the number of equi joins in the rules, the count of
    rules, the number of fields that are allowed to vary, and the number of rows.

    Args:
        br_combination (List[Dict]): The combination of rows outputted by
            find_blocking_rules_below_threshold_comparison_count.
        max_comparison_count (int): The maximum comparison count amongst the rules.
            This is needed to normalise the cost of more or fewer comparison rows.
        num_equi_join_weight (Union[int, float], optional): The weight for num_equi_join
            Defaults to 1.
        field_freedom_weight (Union[int, float], optional): The weight for field
            freedom. Defaults to 1.
        num_brs_weight (Union[int, float], optional): The weight for the number of
            blocking rules found. Defaults to 1.
        num_comparison_weight (Union[int, float], optional): The weight for the
            number of comparison rows. Defaults to 1.

    Returns:
        dict: The calculated cost and individual component costs.
    """

    num_equi_join_cost = sum(row["num_equi_joins"] for row in br_combination)
    total_row_count = sum(row["comparison_count"] for row in br_combination)
    normalised_row_count = total_row_count / max_comparison_count

    # We want a better score for br_combinations that allow each field to
    # vary as much as possible.
    field_freedom_cost = calculate_field_freedom_cost(br_combination)
    num_brs_cost = len(br_combination)

    num_equi_join_cost_weighted = num_equi_join_weight * num_equi_join_cost
    field_freedom_cost_weighted = field_freedom_weight * field_freedom_cost
    num_brs_cost_weighted = num_brs_weight * num_brs_cost
    num_comparison_rows_cost_weighted = num_comparison_weight * normalised_row_count

    total_cost = (
        num_equi_join_cost_weighted
        + field_freedom_cost_weighted
        + num_brs_cost_weighted
        + num_comparison_rows_cost_weighted
    )

    return {
        "cost": total_cost,
        "num_equi_join_cost_weighted": num_equi_join_cost_weighted,
        "field_freedom_cost_weighted": field_freedom_cost_weighted,
        "num_brs_cost_weighted": num_brs_cost_weighted,
        "num_comparison_rows_cost_weighted": num_comparison_rows_cost_weighted,
        "num_equi_join_cost": num_equi_join_cost,
        "field_freedom_cost": field_freedom_cost,
        "num_brs_cost": num_brs_cost,
        "num_comparison_rows_cost": normalised_row_count,
    }

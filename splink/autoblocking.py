from random import randint

import pandas as pd


def localised_shuffle(lst: list, window_percent: float) -> list:
    """
    Performs a localised shuffle on a list.

    Args:
        lst (list): The list to shuffle.
        window_percent (float): The window percent for shuffle e.g. 0.3 for shuffle
            within 30% of orig position

    Returns:
        list: A shuffled copy of the original list.
    """
    window_size = max(1, int(window_percent * len(lst)))
    return sorted(lst, key=lambda x: lst.index(x) + randint(-window_size, window_size))


def check_field_freedom(candidate_set, field_names, min_field_freedom):
    """
    Checks if each field in the candidate set is allowed to vary at least 'min_field_freedom' times.

    Args:
        candidate_set (list): The candidate set of rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        bool: True if each field can vary at least 'min_field_freedom' times, False otherwise.
    """
    covered_fields = {field: 0 for field in field_names}
    for row in candidate_set:
        for field in field_names:
            if row[field] == 0:
                covered_fields[field] += 1
    return all(count >= min_field_freedom for count in covered_fields.values())


def heuristic_select_rows(data, field_names, min_field_freedom):
    """
    Implements a heuristic algorithm to select rows. It ensures that each field is allowed
    to vary at least 'min_field_freedom' times.

    Args:
        data (list): The data rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        list: The candidate set of rows.
    """
    data_sorted_randomised = localised_shuffle(data, 0.3)
    candidate_set = []

    for row in data_sorted_randomised:
        candidate_set.append(row)
        if check_field_freedom(candidate_set, field_names, min_field_freedom):
            break

    return candidate_set


def calculate_field_freedom_cost(combination, field_names):
    """
    Calculates the field cost for a given combination of rows. It counts the number of
    times each field is allowed to vary (i.e., not included in the blocking rules).

    Args:
        combination (list): The combination of rows.
        field_names (list): The list of field names.

    Returns:
        int: The field freedom cost.
    """

    # max_cost is num rows time num ields
    field_cost = len(combination) * len(field_names)
    for field in field_names:
        variances = sum(row[field] == 0 for row in combination)
        if variances > 1:
            field_cost -= variances
    return field_cost


def calculate_cost(
    combination,
    field_names,
    complexity_weight=10,
    field_freedom_weight=10,
    row_weight=1000,
):
    """
    Calculates a cost for a given combination of rows. The cost is a weighted sum of the
    complexity, count, number of fields that are allowed to vary, and number of rows.

    Args:
        combination (list): The combination of rows.
        field_names (list): The list of field names.
        complexity_weight (int, optional): The weight for complexity. Defaults to 10.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to 10.
        row_weight (int, optional): The weight for row count. Defaults to 1000.

    Returns:
        dict: The calculated cost and individual component costs.
    """
    complexity_cost = sum(row["complexity"] for row in combination)
    total_row_count = sum(row["comparison_count"] for row in combination)
    field_freedom_cost = calculate_field_freedom_cost(combination, field_names)
    row_cost = len(combination)

    total_cost = (
        complexity_weight * complexity_cost
        + field_freedom_weight * field_freedom_cost
        + row_weight * row_cost
    )

    return {
        "complexity_cost": complexity_weight * complexity_cost,
        "field_freedom_cost": field_freedom_weight * field_freedom_cost,
        "row_cost": row_weight * row_cost,
        "cost": total_cost,
        "total_comparisons_count": total_row_count,
    }


def suggest_blocking_rules_for_prediction(df_blocks_found):
    df_blocks_found = df_blocks_found.sort_values(
        by=["complexity", "comparison_count"], ascending=[True, False]
    )
    blocks_found_recs = df_blocks_found.to_dict(orient="records")

    blocking_cols = list(blocks_found_recs[0].keys())
    blocking_cols = [c for c in blocking_cols if c.startswith("__fixed__")]

    results = []
    for min_freedom in range(1, 4):
        for run in range(5):
            selected_rows = heuristic_select_rows(
                blocks_found_recs, blocking_cols, min_field_freedom=min_freedom
            )
            cost_dict = calculate_cost(selected_rows, blocking_cols)
            cost_dict.update(
                {
                    "run_num": run,
                    "freedom": min_freedom,
                    "blocking_rules": " || ".join(
                        [row["blocking_rules"] for row in selected_rows]
                    ),
                }
            )
            results.append(cost_dict)

    results_df = pd.DataFrame(results)
    min_scores_df = (
        results_df.sort_values("cost").groupby("freedom", as_index=False).first()
    )
    return min_scores_df

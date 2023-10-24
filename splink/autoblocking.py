import logging
from random import randint

import pandas as pd

logger = logging.getLogger(__name__)


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
    Checks if each field in the candidate set is allowed to vary at least
    'min_field_freedom' times.

    Args:
        candidate_set (list): The candidate set of rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        bool: True if each field can vary at least 'min_field_freedom' times,
            False otherwise.
    """
    covered_fields = {field: 0 for field in field_names}
    for row in candidate_set:
        for field in field_names:
            if row[field] == 0:
                covered_fields[field] += 1
    return all(count >= min_field_freedom for count in covered_fields.values())


def heuristic_select_brs_that_have_min_freedom(data, field_names, min_field_freedom):
    """
    A heuristic algorithm to select blocking rules that between them
    ensure that each field is allowed to vary at least 'min_field_freedom' times.

    Args:
        data (list): The data rows.
        field_names (list): The list of field names.
        min_field_freedom (int): The minimum field freedom.

    Returns:
        list: The candidate set of rows.
    """
    data_sorted_randomised = localised_shuffle(data, 0.3)
    candidate_rows = []

    for row in data_sorted_randomised:
        candidate_rows.append(row)
        if check_field_freedom(candidate_rows, field_names, min_field_freedom):
            break

    sorted_candidate_rows = sorted(candidate_rows, key=lambda x: x["blocking_columns"])

    return sorted_candidate_rows


def calculate_field_freedom_cost(combination_of_brs, field_names):
    """
    We want a better score for combination_of_brss that allow each field to
    vary as much as possible.

    e.g. we don't like combiantions of four rules
    that holds first_name and surname constant in 3 out of four
    and only allows them to vary in one, even if that affords greater
    variance to other

    Calculates the field cost for a given combination of brs. It counts the number of
    times each field is allowed to vary (i.e., not included in the blocking rules).

    Args:
        combination_of_brs (list): The combination_of_brs .
        field_names (list): The list of field names.

    Returns:
        int: The field freedom cost.
    """

    total_cost = 0
    for field in field_names:
        field_can_vary_count = sum(row[field] == 0 for row in combination_of_brs)

        costs_by_count = {0: 20, 1: 10, 2: 2, 3: 1, 4: 1}
        cost = costs_by_count.get(field_can_vary_count, 0) / 10

        total_cost = total_cost + cost

    return total_cost


def calculate_cost(
    combination,
    field_names,
    max_comparison_count,
    complexity_weight,
    field_freedom_weight,
    num_brs_weight,
    num_comparison_weight,
):
    """
    Calculates a cost for a given combination of rows. The cost is a weighted sum of the
    complexity, count, number of fields that are allowed to vary, and number of rows.

    Args:
        combination (list): The combination of rows.
        field_names (list): The list of field names.
        complexity_weight (int, optional): The weight for complexity. Defaults to 10.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to
            10.
        num_brs_weight (int, optional): The weight for the number of blocking rules
            found. Defaults to 1000.

    Returns:
        dict: The calculated cost and individual component costs.
    """
    # Complexity is the number of fields held constant in a given blocking rule
    complexity_cost = sum(row["complexity"] for row in combination)
    total_row_count = sum(row["comparison_count"] for row in combination)
    normalised_row_count = total_row_count / max_comparison_count

    # We want a better score for combinations that allow each field to
    # vary as much as possible.
    field_freedom_cost = calculate_field_freedom_cost(combination, field_names)
    num_brs_cost = len(combination)

    total_cost = (
        complexity_weight * complexity_cost
        + field_freedom_weight * field_freedom_cost
        + num_brs_weight * num_brs_cost
        + num_comparison_weight * normalised_row_count
    )

    return {
        "cost": total_cost,
        "complexity_cost_weighted": complexity_weight * complexity_cost,
        "field_freedom_cost_weighted": field_freedom_weight * field_freedom_cost,
        "num_brs_cost_weighted": num_brs_weight * num_brs_cost,
        "num_comparison_rows_cost_weighted": num_comparison_weight
        * normalised_row_count,
        "complexity_cost": complexity_cost,
        "field_freedom_cost": field_freedom_cost,
        "num_brs_cost": num_brs_cost,
        "num_comparison_rows_cost": normalised_row_count,
        "total_comparisons_count": total_row_count,
    }


def get_block_on_string(br_rows):
    block_on_strings = []

    for row in br_rows:
        quoted_args = []
        for arg in row["blocking_columns"]:
            quoted_arg = f'"{arg}"'
            quoted_args.append(quoted_arg)

        block_on_args = ", ".join(quoted_args)
        block_on_string = f"block_on({block_on_args})"
        block_on_strings.append(block_on_string)

    return " \n".join(block_on_strings)


def get_em_training_string(br_rows):
    block_on_strings = []

    for row in br_rows:
        quoted_args = []
        for arg in row["blocking_columns"]:
            quoted_arg = f'"{arg}"'
            quoted_args.append(quoted_arg)

        block_on_args = ", ".join(quoted_args)
        block_on_string = f"block_on({block_on_args})"
        block_on_strings.append(block_on_string)

    training_statements = []
    for block_on_str in block_on_strings:
        statement = (
            f"linker.estimate_parameters_using_expectation_maximisation({block_on_str})"
        )
        training_statements.append(statement)

    return " \n".join(training_statements)


def suggest_blocking_rules(
    df_block_stats,
    min_freedom=1,
    num_runs=5,
    complexity_weight=0,
    field_freedom_weight=1,
    num_brs_weight=10,
    num_comparison_weight=10,
):
    """Use a cost optimiser to suggest blocking rules

    Args:
        df_block_stats: Dataframe returned by find_blocking_rules_below_threshold
        min_freedom (int, optional): Each column should have at least this many
            opportunities to vary amongst the blocking rules. Defaults to 1.
        num_runs (int, optional): How many random combinations of
            rules to try.  The best will be selected. Defaults to 5.
        complexity_weight (int, optional): The weight for complexity. Defaults to 10.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to
            10.
        num_brs_weight (int, optional): The weight for the number of blocking rules
            found. Defaults to 10.

    Returns:
        pd.DataFrame: A DataFrame containing the results of the blocking rules
            suggestion. It includes columns such as
            'suggested_blocking_rules_for_prediction',
            'suggested_EM_training_statements', and various cost information

    """
    if len(df_block_stats) == 0:
        return None

    max_comparison_count = df_block_stats["comparison_count"].max()

    df_block_stats = df_block_stats.sort_values(
        by=["complexity", "comparison_count"], ascending=[True, False]
    )
    blocks_found_recs = df_block_stats.to_dict(orient="records")

    blocking_cols = list(blocks_found_recs[0].keys())
    blocking_cols = [c for c in blocking_cols if c.startswith("__fixed__")]

    results = []

    for run in range(num_runs):
        selected_rows = heuristic_select_brs_that_have_min_freedom(
            blocks_found_recs, blocking_cols, min_field_freedom=min_freedom
        )

        cost_dict = {
            "suggested_blocking_rules_for_prediction": get_block_on_string(
                selected_rows
            ),
            "suggested_EM_training_statements": get_em_training_string(selected_rows),
        }

        costs = calculate_cost(
            selected_rows,
            blocking_cols,
            max_comparison_count,
            complexity_weight,
            field_freedom_weight,
            num_brs_weight,
            num_comparison_weight,
        )

        cost_dict.update(costs)
        cost_dict.update(
            {
                "run_num": run,
                "minimum_freedom_for_each_column": min_freedom,
                "suggested_blocking_rules_as_splink_brs": [
                    row["splink_blocking_rule"] for row in selected_rows
                ],
            }
        )
        results.append(cost_dict)

    results_df = pd.DataFrame(results)
    # easier to read if we normalise the cost to the best is 0
    min_ = results_df["field_freedom_cost"].min()
    results_df["field_freedom_cost"] = results_df["field_freedom_cost"] - min_

    min_ = results_df["field_freedom_cost_weighted"].min()
    results_df["field_freedom_cost_weighted"] = (
        results_df["field_freedom_cost_weighted"] - min_
    )
    results_df["cost"] = results_df["cost"] - min_

    min_scores_df = results_df.sort_values("cost")
    min_scores_df = min_scores_df.drop_duplicates(
        "suggested_blocking_rules_for_prediction"
    )

    return min_scores_df

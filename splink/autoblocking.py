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

    logger.debug("--")
    logger.debug("Beginning calculating field freedom cost")

    for row in combination_of_brs:
        logger.debug(f"  {row['blocking_columns']}")

    total_cost = 0
    for field in field_names:
        field_can_vary_count = sum(row[field] == 0 for row in combination_of_brs)

        costs_by_count = {0: 20, 1: 10, 2: 2, 3: 1, 4: 1}
        cost = costs_by_count.get(field_can_vary_count, 0) / 10
        logger.debug(f"{field} can vary in {field_can_vary_count} rules, cost: {cost}")

        total_cost = total_cost + cost
    logger.debug(f"Ending: total cost = {total_cost}")
    logger.debug("--")
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


def suggest_blocking_rules_for_prediction(
    df_block_stats,
    min_freedom=1,
    num_runs=5,
    complexity_weight=0,
    field_freedom_weight=1,
    num_brs_weight=10,
    num_comparison_weight=10,
):
    """Use a cost optimiser to suggest blocking rules for prediction

    Args:
        df_block_stats: Dataframe returned by find_blocking_rules_below_threshold
        min_freedom (int, optional): Each column should have at least this many
            opportunities to vary amongst the blockign rules. Defaults to 1.
        num_runs (int, optional): How many random combinations of
            rules to try.  The best will be selected. Defaults to 5.
        complexity_weight (int, optional): The weight for complexity. Defaults to 10.
        field_freedom_weight (int, optional): The weight for field freedom. Defaults to
            10.
        num_brs_weight (int, optional): The weight for the number of blocking rules
            found. Defaults to 1000.

    Returns:
        _type_: _description_
    """

    max_comparison_count = df_block_stats["comparison_count"].max()

    df_block_stats = df_block_stats.sort_values(
        by=["complexity", "comparison_count"], ascending=[True, False]
    )
    blocks_found_recs = df_block_stats.to_dict(orient="records")

    blocking_cols = list(blocks_found_recs[0].keys())
    blocking_cols = [c for c in blocking_cols if c.startswith("__fixed__")]

    results = []

    for run in range(num_runs):
        selected_rows = heuristic_select_rows(
            blocks_found_recs, blocking_cols, min_field_freedom=min_freedom
        )
        cost_dict = calculate_cost(
            selected_rows,
            blocking_cols,
            max_comparison_count,
            complexity_weight,
            field_freedom_weight,
            num_brs_weight,
            num_comparison_weight,
        )
        cost_dict.update(
            {
                "run_num": run,
                "minimum_freedom_for_each_column": min_freedom,
                "suggested_blocking_rules_string": " || ".join(
                    [" AND ".join(row["blocking_columns"]) for row in selected_rows]
                ),
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
    min_scores_df = min_scores_df.drop_duplicates("suggested_blocking_rules_string")

    return min_scores_df


def blocking_rules_for_prediction_report(suggested_blocking_rules_dict):
    d = suggested_blocking_rules_dict
    serialized_brs = [f"{br.sql}" for br in d["suggested_blocking_rules_as_splink_brs"]]

    lines = []
    lines.append("Blocking Rules for Prediction Report")
    lines.append("=====================================")
    lines.append("Recommended blocking rules for prediction:")
    lines.append("")
    lines.extend(serialized_brs)
    lines.append("")

    msg1 = f"These will generate {d['total_comparisons_count']:,.0f} comparisons"
    msg2 = "prior to deduping"
    lines.append(f"{msg1} {msg2}")
    lines.append("")

    msg = "Each column can vary in at least"
    lines.append(f"{msg} {d['minimum_freedom_for_each_column']} different rules")
    lines.append(f"Complexity Cost: {d['complexity_cost']}")
    lines.append(f"Field Freedom Cost: {d['field_freedom_cost']}")
    lines.append(f"Num BRs Cost: {d['num_brs_cost']}")
    lines.append(f"Total Cost: {d['cost']}")

    lines.append(f"Run Num: {d['run_num']}")

    report = "\n".join(lines)
    return report
